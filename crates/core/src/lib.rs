//! Deterministic validation for Pontmore public protocol objects.
//!
//! The crate validates PIP-00 and PIP-01 content and indexing tags, verifies signed
//! PIP-02 events, and reconstructs `pontmore/swap@1` coordination chains.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use url::Url;

mod pip02;

pub use pip02::{
    CoordinationState, DescriptorBinding, EventRejection, ForkEvidence, Reconstruction,
    ResolutionEffect, validate_swap_coordination,
};

/// A decoded Nostr tag.
pub type Tag = Vec<String>;

/// Stable validation error identifiers intended for compatibility vectors and APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidContent,
    UnsupportedVersion,
    MissingDTag,
    MissingAgentTag,
    EmptyCapabilities,
    InvalidCapabilityId,
    CapabilityTagMissing,
    CapabilityTagUndeclared,
    EmptyEscrowType,
    EscrowTypeNotLowercase,
    EmptyNetworks,
    EmptyNetworkId,
    NetworkIdNotLowercase,
    NetworkTagUndeclared,
    RequiredNetworkMissing,
    UnsupportedSchemaType,
    InvalidSchemaUrl,
    DescriptorNotCurrent,
    DescriptorExpired,
    InvalidEvent,
    InvalidEventKind,
    UnsupportedProfile,
    InvalidDescriptorBinding,
    InvalidRoles,
    InvalidTerms,
    InvalidCommitment,
    InvalidActionTags,
    UnknownAction,
    UnauthorizedSigner,
    InvalidActionData,
    InvalidTransition,
    ActionAfterTerminal,
    DuplicateEvent,
    OutOfOrderAction,
}

/// One deterministic validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationError {
    pub code: ErrorCode,
    pub path: String,
    pub message: String,
}

impl ValidationError {
    pub(crate) fn new(
        code: ErrorCode,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            path: path.into(),
            message: message.into(),
        }
    }
}

/// PIP-00 version 1 content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDefinition {
    pub version: u64,
    pub capabilities: Vec<String>,
}

/// PIP-01 version 1 content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EscrowDescriptor {
    pub version: u64,
    pub escrow_type: String,
    pub networks: Vec<String>,
    #[serde(default)]
    pub service: Option<EscrowService>,
    pub expires_at: u64,
}

/// The only PIP-01 service field currently defined.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EscrowService {
    pub schema: ServiceSchema,
}

/// A machine-readable service-schema reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceSchema {
    #[serde(rename = "type")]
    pub schema_type: String,
    pub url: String,
}

/// Caller-supplied facts needed to decide whether a descriptor can be selected.
///
/// Determining the current addressable revision requires relay/index context and
/// therefore remains outside the pure descriptor-content validator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorSelection {
    pub validation_time: u64,
    pub is_current_revision: bool,
}

/// Parse and validate PIP-00 Agent-definition content and its Nostr tags.
#[must_use]
pub fn validate_agent_definition(content: &str, tags: &[Tag]) -> Vec<ValidationError> {
    let definition = match serde_json::from_str::<AgentDefinition>(content) {
        Ok(definition) => definition,
        Err(error) => {
            return vec![ValidationError::new(
                ErrorCode::InvalidContent,
                "content",
                format!("invalid Agent-definition content: {error}"),
            )];
        }
    };

    let mut errors = Vec::new();
    if definition.version != 1 {
        errors.push(ValidationError::new(
            ErrorCode::UnsupportedVersion,
            "content.version",
            format!("unsupported PIP-00 content version: {}", definition.version),
        ));
    }
    if !has_nonempty_tag(tags, "d") {
        errors.push(ValidationError::new(
            ErrorCode::MissingDTag,
            "tags",
            "Agent definition requires a non-empty d tag",
        ));
    }
    if !has_exact_tag_value(tags, "t", "agent") {
        errors.push(ValidationError::new(
            ErrorCode::MissingAgentTag,
            "tags",
            "Agent definition requires [\"t\", \"agent\"]",
        ));
    }
    if definition.capabilities.is_empty() {
        errors.push(ValidationError::new(
            ErrorCode::EmptyCapabilities,
            "content.capabilities",
            "capabilities must be non-empty",
        ));
    }

    let declared: BTreeSet<&str> = definition.capabilities.iter().map(String::as_str).collect();
    for (index, capability) in definition.capabilities.iter().enumerate() {
        if !valid_versioned_identifier(capability) {
            errors.push(ValidationError::new(
                ErrorCode::InvalidCapabilityId,
                format!("content.capabilities[{index}]"),
                format!("invalid versioned capability identifier: {capability}"),
            ));
        }
        let tag_value = format!("pontmore-capability:{capability}");
        if !has_exact_tag_value(tags, "t", &tag_value) {
            errors.push(ValidationError::new(
                ErrorCode::CapabilityTagMissing,
                format!("content.capabilities[{index}]"),
                format!("missing capability index tag for {capability}"),
            ));
        }
    }

    for value in
        tag_values(tags, "t").filter_map(|value| value.strip_prefix("pontmore-capability:"))
    {
        if !declared.contains(value) {
            errors.push(ValidationError::new(
                ErrorCode::CapabilityTagUndeclared,
                "tags",
                format!("capability tag is not declared in content: {value}"),
            ));
        }
    }

    errors
}

/// Parse and validate PIP-01 descriptor content, tags, and optional selection facts.
#[must_use]
pub fn validate_escrow_descriptor(
    content: &str,
    tags: &[Tag],
    selection: Option<DescriptorSelection>,
) -> Vec<ValidationError> {
    let descriptor = match serde_json::from_str::<EscrowDescriptor>(content) {
        Ok(descriptor) => descriptor,
        Err(error) => {
            return vec![ValidationError::new(
                ErrorCode::InvalidContent,
                "content",
                format!("invalid escrow-descriptor content: {error}"),
            )];
        }
    };

    let mut errors = Vec::new();
    validate_descriptor_fields(&descriptor, tags, &mut errors);
    validate_service_schema(descriptor.service.as_ref(), &mut errors);
    validate_descriptor_selection(&descriptor, selection, &mut errors);
    errors
}

fn validate_descriptor_fields(
    descriptor: &EscrowDescriptor,
    tags: &[Tag],
    errors: &mut Vec<ValidationError>,
) {
    if descriptor.version != 1 {
        errors.push(ValidationError::new(
            ErrorCode::UnsupportedVersion,
            "content.version",
            format!("unsupported PIP-01 content version: {}", descriptor.version),
        ));
    }
    if !has_nonempty_tag(tags, "d") {
        errors.push(ValidationError::new(
            ErrorCode::MissingDTag,
            "tags",
            "escrow descriptor requires a non-empty d tag",
        ));
    }
    validate_lowercase_identifier(
        &descriptor.escrow_type,
        "content.escrow_type",
        ErrorCode::EmptyEscrowType,
        ErrorCode::EscrowTypeNotLowercase,
        errors,
    );
    if descriptor.networks.is_empty() {
        errors.push(ValidationError::new(
            ErrorCode::EmptyNetworks,
            "content.networks",
            "networks must be non-empty",
        ));
    }
    for (index, network) in descriptor.networks.iter().enumerate() {
        validate_lowercase_identifier(
            network,
            format!("content.networks[{index}]"),
            ErrorCode::EmptyNetworkId,
            ErrorCode::NetworkIdNotLowercase,
            errors,
        );
    }

    let networks: BTreeSet<&str> = descriptor.networks.iter().map(String::as_str).collect();
    for value in tag_values(tags, "t").filter_map(|value| value.strip_prefix("pontmore-network:")) {
        if !networks.contains(value) {
            errors.push(ValidationError::new(
                ErrorCode::NetworkTagUndeclared,
                "tags",
                format!("network tag is not declared in content: {value}"),
            ));
        }
    }

    let required_network = match descriptor.escrow_type.as_str() {
        "lightning_hold_invoice" => Some("lightning"),
        "cashu_escrow" => Some("cashu"),
        _ => None,
    };
    if let Some(required) = required_network
        && !networks.contains(required)
    {
        errors.push(ValidationError::new(
            ErrorCode::RequiredNetworkMissing,
            "content.networks",
            format!("{} requires the {required} network", descriptor.escrow_type),
        ));
    }
}

fn validate_service_schema(service: Option<&EscrowService>, errors: &mut Vec<ValidationError>) {
    if let Some(service) = service {
        if !matches!(service.schema.schema_type.as_str(), "openapi" | "asyncapi") {
            errors.push(ValidationError::new(
                ErrorCode::UnsupportedSchemaType,
                "content.service.schema.type",
                format!("unsupported schema type: {}", service.schema.schema_type),
            ));
        }
        match Url::parse(&service.schema.url) {
            Ok(url) if url.scheme() == "https" && url.host_str().is_some() => {}
            _ => errors.push(ValidationError::new(
                ErrorCode::InvalidSchemaUrl,
                "content.service.schema.url",
                "service schema URL must be an absolute https URL",
            )),
        }
    }
}

fn validate_descriptor_selection(
    descriptor: &EscrowDescriptor,
    selection: Option<DescriptorSelection>,
    errors: &mut Vec<ValidationError>,
) {
    if let Some(selection) = selection {
        if !selection.is_current_revision {
            errors.push(ValidationError::new(
                ErrorCode::DescriptorNotCurrent,
                "selection.is_current_revision",
                "descriptor is not the current addressable-event revision",
            ));
        }
        if selection.validation_time >= descriptor.expires_at {
            errors.push(ValidationError::new(
                ErrorCode::DescriptorExpired,
                "content.expires_at",
                "descriptor is expired at the supplied validation time",
            ));
        }
    }
}

fn has_nonempty_tag(tags: &[Tag], name: &str) -> bool {
    tag_values(tags, name).any(|value| !value.is_empty())
}

fn has_exact_tag_value(tags: &[Tag], name: &str, expected: &str) -> bool {
    tag_values(tags, name).any(|value| value == expected)
}

fn tag_values<'a>(tags: &'a [Tag], name: &'a str) -> impl Iterator<Item = &'a str> {
    tags.iter().filter_map(move |tag| match tag.as_slice() {
        [tag_name, value, ..] if tag_name == name => Some(value.as_str()),
        _ => None,
    })
}

fn valid_versioned_identifier(value: &str) -> bool {
    let Some((name, version)) = value.rsplit_once('@') else {
        return false;
    };
    let Some((namespace, capability)) = name.split_once('/') else {
        return false;
    };
    !namespace.is_empty()
        && !capability.is_empty()
        && !capability.contains('/')
        && version.parse::<u64>().is_ok_and(|version| version > 0)
}

fn validate_lowercase_identifier(
    value: &str,
    path: impl Into<String>,
    empty_code: ErrorCode,
    lowercase_code: ErrorCode,
    errors: &mut Vec<ValidationError>,
) {
    let path = path.into();
    if value.is_empty() {
        errors.push(ValidationError::new(
            empty_code,
            path,
            "identifier must be non-empty",
        ));
    } else if value.chars().any(char::is_uppercase) {
        errors.push(ValidationError::new(
            lowercase_code,
            path,
            format!("identifier must be lowercase: {value}"),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_agent_definition() {
        let content = r#"{"version":1,"capabilities":["pontmore/swap@1"]}"#;
        let tags = vec![
            vec!["d".into(), "agent".into()],
            vec!["t".into(), "agent".into()],
            vec!["t".into(), "pontmore-capability:pontmore/swap@1".into()],
        ];

        assert!(validate_agent_definition(content, &tags).is_empty());
    }

    #[test]
    fn rejects_descriptor_at_expiry_boundary() {
        let content =
            r#"{"version":1,"escrow_type":"cashu_escrow","networks":["cashu"],"expires_at":100}"#;
        let tags = vec![vec!["d".into(), "cashu-main".into()]];
        let selection = DescriptorSelection {
            validation_time: 100,
            is_current_revision: true,
        };

        let errors = validate_escrow_descriptor(content, &tags, Some(selection));
        assert_eq!(errors[0].code, ErrorCode::DescriptorExpired);
    }

    #[test]
    fn identifier_requires_a_positive_integer_version() {
        assert!(valid_versioned_identifier("pontmore/swap@1"));
        assert!(!valid_versioned_identifier("pontmore/swap@0"));
        assert!(!valid_versioned_identifier("swap@1"));
        assert!(!valid_versioned_identifier("pontmore/swap@draft"));
    }
}
