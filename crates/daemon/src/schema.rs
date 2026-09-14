use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use reqwest::{Client, header::CONTENT_TYPE, redirect::Policy};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::net::lookup_host;
use url::Url;

#[derive(Debug, Clone)]
pub struct SchemaArtifact {
    pub sha256: String,
    pub media_type: String,
    pub body: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("invalid schema URL: {0}")]
    Url(#[from] url::ParseError),
    #[error("schema URL must use HTTPS and contain a public DNS hostname")]
    UnsafeUrl,
    #[error("schema hostname resolved to an unsafe address")]
    UnsafeAddress,
    #[error("schema DNS lookup failed: {0}")]
    Dns(#[from] std::io::Error),
    #[error("schema HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("schema response was not successful: {0}")]
    Status(reqwest::StatusCode),
    #[error("schema response exceeded {0} bytes")]
    TooLarge(usize),
    #[error("schema response has unsupported content type `{0}`")]
    ContentType(String),
    #[error("schema document is not a JSON object")]
    InvalidDocument,
}

#[derive(Clone)]
pub struct SchemaFetcher {
    timeout: Duration,
    max_bytes: usize,
}

impl SchemaFetcher {
    #[must_use]
    pub const fn new(timeout: Duration, max_bytes: usize) -> Self {
        Self { timeout, max_bytes }
    }

    /// Fetch and validate a schema with SSRF and resource limits.
    ///
    /// # Errors
    /// Returns URL, DNS, HTTP, size, media-type, or document errors.
    pub async fn fetch(
        &self,
        raw_url: &str,
        schema_type: &str,
    ) -> Result<SchemaArtifact, SchemaError> {
        let url = Url::parse(raw_url)?;
        if url.scheme() != "https" || url.username() != "" || url.password().is_some() {
            return Err(SchemaError::UnsafeUrl);
        }
        let host = url.host_str().ok_or(SchemaError::UnsafeUrl)?;
        if host.parse::<IpAddr>().is_ok() {
            return Err(SchemaError::UnsafeUrl);
        }
        let port = url.port_or_known_default().ok_or(SchemaError::UnsafeUrl)?;
        let addresses = lookup_host((host, port))
            .await?
            .collect::<Vec<SocketAddr>>();
        if addresses.is_empty() || addresses.iter().any(|address| !is_public(address.ip())) {
            return Err(SchemaError::UnsafeAddress);
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .timeout(self.timeout)
            .resolve_to_addrs(host, &addresses)
            .build()?;
        let mut response = client.get(url).send().await?;
        if !response.status().is_success() {
            return Err(SchemaError::Status(response.status()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_bytes as u64)
        {
            return Err(SchemaError::TooLarge(self.max_bytes));
        }
        let media_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if !matches!(
            media_type.as_str(),
            "application/json" | "application/schema+json" | "application/yaml" | "text/yaml"
        ) {
            return Err(SchemaError::ContentType(media_type));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if body.len().saturating_add(chunk.len()) > self.max_bytes {
                return Err(SchemaError::TooLarge(self.max_bytes));
            }
            body.extend_from_slice(&chunk);
        }
        validate_document(&body, &media_type, schema_type)?;
        Ok(SchemaArtifact {
            sha256: hex::encode(Sha256::digest(&body)),
            media_type,
            body,
        })
    }
}

fn validate_document(body: &[u8], media_type: &str, schema_type: &str) -> Result<(), SchemaError> {
    let value: serde_json::Value = if media_type.contains("json") {
        serde_json::from_slice(body).map_err(|_| SchemaError::InvalidDocument)?
    } else {
        serde_yaml::from_slice(body).map_err(|_| SchemaError::InvalidDocument)?
    };
    let object = value.as_object().ok_or(SchemaError::InvalidDocument)?;
    let expected = match schema_type {
        "openapi" => "openapi",
        "asyncapi" => "asyncapi",
        _ => return Err(SchemaError::InvalidDocument),
    };
    if object
        .get(expected)
        .and_then(serde_json::Value::as_str)
        .is_none()
    {
        return Err(SchemaError::InvalidDocument);
    }
    Ok(())
}

fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_documentation()
                || octets[0] == 0
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 198 && (18..=19).contains(&octets[1]))
                || octets[0] >= 240)
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_public(IpAddr::V4(mapped));
            }
            !(ip.is_loopback()
                || ip.is_multicast()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_public_addresses() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "100.64.0.1",
            "169.254.1.1",
            "198.18.0.1",
            "::1",
            "fc00::1",
            "::ffff:127.0.0.1",
            "2001:db8::1",
        ] {
            assert!(!is_public(address.parse().expect("address")));
        }
        assert!(is_public("1.1.1.1".parse().expect("address")));
    }
}
