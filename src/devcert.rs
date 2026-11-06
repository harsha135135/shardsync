//! Development certificates and tokens for local demonstrations and tests.
//!
//! `shardsync dev-setup` writes a throwaway CA, a receiver certificate for `localhost` and
//! `127.0.0.1`, one bearer token, and the receiver's token file. Production deployments must
//! use their own CA and secrets; nothing here is a security boundary for real data.

use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use serde::{Deserialize, Serialize};

use crate::manifest::sha256_hex;

/// The receiver's token file: owners and the SHA-256 of their tokens (never the tokens).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokensFile {
    pub clients: Vec<ClientToken>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientToken {
    pub owner: String,
    pub token_sha256: String,
}

pub struct DevMaterial {
    pub ca_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
}

/// Generates a CA and a receiver certificate signed by it.
pub fn generate_certificates() -> Result<DevMaterial, String> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).map_err(|e| e.to_string())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "ShardSync development CA");
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate().map_err(|e| e.to_string())?;
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).map_err(|e| e.to_string())?;

    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .map_err(|e| e.to_string())?;
    server_params
        .distinguished_name
        .push(DnType::CommonName, "ShardSync development receiver");
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().map_err(|e| e.to_string())?;
    let server_cert = server_params
        .signed_by(&server_key, &ca)
        .map_err(|e| e.to_string())?;
    Ok(DevMaterial {
        ca_pem: ca.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
    })
}

/// A new random bearer token (256 bits, hex).
pub fn new_token() -> String {
    hex::encode(rand::random::<[u8; 32]>())
}

/// Writes a file readable only by its owner.
pub fn write_private(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents)?;
    file.sync_all()
}

/// Writes `ca.pem`, `server-cert.pem`, `server-key.pem`, `tokens.json` and one
/// `token-<owner>.txt` per owner into `out_dir`.
pub fn write_dev_setup(out_dir: &Path, owners: &[String]) -> Result<(), String> {
    if owners.is_empty() {
        return Err("at least one owner is required".into());
    }
    for owner in owners {
        if owner.is_empty()
            || owner.len() > 64
            || !owner
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(format!(
                "owner {owner:?} must be 1-64 letters, digits, '-' or '_'"
            ));
        }
    }
    std::fs::create_dir_all(out_dir).map_err(|e| format!("create {}: {e}", out_dir.display()))?;
    let material = generate_certificates()?;
    let write = |name: &str, contents: &[u8], private: bool| -> Result<(), String> {
        let path = out_dir.join(name);
        let result = if private {
            write_private(&path, contents)
        } else {
            std::fs::write(&path, contents)
        };
        result.map_err(|e| format!("write {}: {e}", path.display()))
    };
    write("ca.pem", material.ca_pem.as_bytes(), false)?;
    write(
        "server-cert.pem",
        material.server_cert_pem.as_bytes(),
        false,
    )?;
    write("server-key.pem", material.server_key_pem.as_bytes(), true)?;
    let mut clients = Vec::new();
    for owner in owners {
        let token = new_token();
        write(&format!("token-{owner}.txt"), token.as_bytes(), true)?;
        clients.push(ClientToken {
            owner: owner.clone(),
            token_sha256: sha256_hex(token.as_bytes()),
        });
    }
    let tokens = serde_json::to_vec_pretty(&TokensFile { clients }).map_err(|e| e.to_string())?;
    write("tokens.json", &tokens, true)?;
    Ok(())
}
