//! PIP-02 version 2 reconstruction for the experimental `pontmore/swap@1` profile.

use std::collections::{BTreeMap, BTreeSet};

use nostr::prelude::{Event, Kind};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{ErrorCode, ValidationError};

const ROOT_KIND: Kind = Kind::Custom(7300);
const ACTION_KIND: Kind = Kind::Custom(7301);
const SWAP_PROFILE: &str = "pontmore/swap@1";

/// Externally established PIP-01 facts needed to validate a coordination root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorBinding {
    pub event_id: String,
    pub coordinate: String,
    pub created_at: u64,
    pub selectable_at_root: bool,
    pub current_at_acceptance: bool,
    pub expires_at: u64,
    pub networks: BTreeSet<String>,
    pub authorities_validated: bool,
    pub supported_resolution_effects: BTreeSet<ResolutionEffect>,
}

/// PIP-02 dispute-resolution effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionEffect {
    Resume,
    AuthorizeSettlement,
    AuthorizeRefund,
    Cancel,
}

/// Public state derived from the validated root and linked action history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationState {
    Proposed,
    Accepted,
    Secured,
    FiatSent,
    FiatConfirmed,
    SettlementAuthorized,
    RefundAuthorized,
    Disputed,
    Declined,
    Cancelled,
    Expired,
    Settled,
    Refunded,
    Forked,
}

impl CoordinationState {
    const fn terminal(self) -> bool {
        matches!(
            self,
            Self::Declined | Self::Cancelled | Self::Expired | Self::Settled | Self::Refunded
        )
    }
}

/// An input event that could not join the validated chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRejection {
    pub event_id: String,
    pub code: ErrorCode,
    pub message: String,
}

/// Both otherwise-valid siblings retained when a fork is detected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkEvidence {
    pub predecessor: String,
    pub siblings: Vec<String>,
}

/// Deterministic reconstruction result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reconstruction {
    pub coordination_id: String,
    pub state: CoordinationState,
    pub tip: String,
    pub applied: Vec<String>,
    pub rejections: Vec<EventRejection>,
    pub fork: Option<ForkEvidence>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootContent {
    version: u64,
    profile: String,
    terms: SwapTerms,
    expires_at: u64,
    #[serde(default)]
    commitments: BTreeMap<String, Commitment>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Commitment {
    algorithm: String,
    digest: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SwapTerms {
    direction: Direction,
    fiat: FiatTerms,
    bitcoin: BitcoinTerms,
    payment_channel: String,
    deadlines: Deadlines,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Direction {
    FiatToBtc,
    BtcToFiat,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FiatTerms {
    currency: String,
    amount: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BitcoinTerms {
    amount: String,
    unit: String,
    network: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Deadlines {
    fiat_pay_by: u64,
    fiat_confirm_by: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActionContent {
    version: u64,
    action: String,
    #[serde(default)]
    data: Option<Value>,
}

#[derive(Debug, Clone)]
struct Roles {
    agent: String,
    customer: String,
    escrow: String,
    resolver: String,
    proposer: String,
}

#[derive(Debug, Clone)]
struct ParsedAction {
    event: Event,
    content: ActionContent,
    predecessor: String,
}

#[derive(Debug, Clone)]
struct Machine {
    state: CoordinationState,
    resume_state: Option<CoordinationState>,
    payment_reference: Option<String>,
    secured: bool,
}

/// Verify and reconstruct one PIP-02 version 2 `pontmore/swap@1` history.
///
/// Events may be supplied in any order. The predecessor graph determines order.
/// The descriptor binding contains facts established by PIP-01 discovery and the
/// selected service schema; this function never infers those facts from relay order.
///
/// # Errors
///
/// Returns root-level validation errors when reconstruction cannot safely begin.
pub fn validate_swap_coordination(
    root: &Event,
    actions: &[Event],
    descriptor: &DescriptorBinding,
) -> Result<Reconstruction, Vec<ValidationError>> {
    let (content, roles) = validate_root(root, descriptor)?;
    let root_id = root.id.to_hex();
    let (children, mut rejections) = parse_actions(actions, &root_id, &roles);
    let mut machine = Machine {
        state: CoordinationState::Proposed,
        resume_state: None,
        payment_reference: None,
        secured: false,
    };
    let mut tip = root_id.clone();
    let mut applied = Vec::new();
    let mut processed = BTreeSet::new();

    loop {
        let Some(candidates) = children.get(&tip) else {
            break;
        };
        let mut valid = Vec::new();
        for candidate in candidates {
            processed.insert(candidate.event.id.to_hex());
            match apply_action(&machine, candidate, &content, &roles, descriptor) {
                Ok(next) => valid.push((candidate, next)),
                Err((code, message)) => {
                    reject(&mut rejections, candidate.event.id.to_hex(), code, message);
                }
            }
        }

        if valid.len() > 1 {
            let mut siblings: Vec<String> = valid
                .iter()
                .map(|(action, _)| action.event.id.to_hex())
                .collect();
            siblings.sort();
            rejections.sort_by(|left, right| left.event_id.cmp(&right.event_id));
            return Ok(Reconstruction {
                coordination_id: root_id,
                state: CoordinationState::Forked,
                tip: tip.clone(),
                applied,
                rejections,
                fork: Some(ForkEvidence {
                    predecessor: tip,
                    siblings,
                }),
            });
        }
        let Some((candidate, next)) = valid.pop() else {
            break;
        };
        machine = next;
        tip = candidate.event.id.to_hex();
        applied.push(tip.clone());
    }

    for candidates in children.values() {
        for candidate in candidates {
            let event_id = candidate.event.id.to_hex();
            if !processed.contains(&event_id) && !applied.contains(&event_id) {
                reject(
                    &mut rejections,
                    event_id,
                    ErrorCode::OutOfOrderAction,
                    "action is not reachable from the validated chain tip",
                );
            }
        }
    }
    rejections.sort_by(|left, right| left.event_id.cmp(&right.event_id));

    Ok(Reconstruction {
        coordination_id: root_id,
        state: machine.state,
        tip,
        applied,
        rejections,
        fork: None,
    })
}

fn parse_actions(
    actions: &[Event],
    root_id: &str,
    roles: &Roles,
) -> (BTreeMap<String, Vec<ParsedAction>>, Vec<EventRejection>) {
    let mut rejections = Vec::new();
    let counts = actions.iter().fold(BTreeMap::new(), |mut counts, event| {
        *counts.entry(event.id.to_hex()).or_insert(0_usize) += 1;
        counts
    });
    let mut duplicate_rejections = BTreeSet::new();
    let mut children: BTreeMap<String, Vec<ParsedAction>> = BTreeMap::new();

    for event in actions {
        let event_id = event.id.to_hex();
        if counts.get(&event_id).copied().unwrap_or_default() > 1 {
            if !duplicate_rejections.insert(event_id.clone()) {
                continue;
            }
            reject(
                &mut rejections,
                event_id,
                ErrorCode::DuplicateEvent,
                "duplicate event ID in reconstruction input",
            );
            continue;
        }
        match parse_action(event, root_id, roles) {
            Ok(action) => children
                .entry(action.predecessor.clone())
                .or_default()
                .push(action),
            Err((code, message)) => reject(&mut rejections, event_id, code, message),
        }
    }

    (children, rejections)
}

fn validate_root(
    root: &Event,
    descriptor: &DescriptorBinding,
) -> Result<(RootContent, Roles), Vec<ValidationError>> {
    let mut errors = Vec::new();
    if root.verify().is_err() {
        errors.push(ValidationError::new(
            ErrorCode::InvalidEvent,
            "root",
            "invalid Nostr event ID or signature",
        ));
    }
    if root.kind != ROOT_KIND {
        errors.push(ValidationError::new(
            ErrorCode::InvalidEventKind,
            "root.kind",
            "coordination root kind must be 7300",
        ));
    }
    let content = match serde_json::from_str::<RootContent>(&root.content) {
        Ok(content) => content,
        Err(error) => {
            errors.push(ValidationError::new(
                ErrorCode::InvalidContent,
                "root.content",
                format!("invalid root content: {error}"),
            ));
            return Err(errors);
        }
    };
    validate_root_content(&content, root.created_at.as_secs(), descriptor, &mut errors);
    validate_descriptor_tags(root, descriptor, &mut errors);
    let roles = validate_roles(root, &mut errors);
    if errors.is_empty() {
        Ok((
            content,
            roles.expect("roles exist when validation succeeds"),
        ))
    } else {
        Err(errors)
    }
}

fn validate_root_content(
    content: &RootContent,
    root_created_at: u64,
    descriptor: &DescriptorBinding,
    errors: &mut Vec<ValidationError>,
) {
    if content.version != 2 {
        errors.push(ValidationError::new(
            ErrorCode::UnsupportedVersion,
            "root.content.version",
            "PIP-02 content version must be 2",
        ));
    }
    if content.profile != SWAP_PROFILE {
        errors.push(ValidationError::new(
            ErrorCode::UnsupportedProfile,
            "root.content.profile",
            format!("unsupported coordination profile: {}", content.profile),
        ));
    }
    validate_terms(content, descriptor, errors);
    for (name, commitment) in &content.commitments {
        if !matches!(name.as_str(), "private_terms" | "quote")
            || commitment.algorithm != "sha256-bytes@1"
            || !valid_sha256_digest(&commitment.digest)
        {
            errors.push(ValidationError::new(
                ErrorCode::InvalidCommitment,
                format!("root.content.commitments.{name}"),
                "unsupported commitment key, algorithm, or digest encoding",
            ));
        }
    }
    if root_descriptor_invalid(root_created_at, descriptor) {
        errors.push(ValidationError::new(
            ErrorCode::InvalidDescriptorBinding,
            "descriptor",
            "descriptor was not selectable and compatible when the root was created",
        ));
    }
}

fn validate_terms(
    content: &RootContent,
    descriptor: &DescriptorBinding,
    errors: &mut Vec<ValidationError>,
) {
    let terms = &content.terms;
    let valid_currency = terms.fiat.currency.len() == 3
        && terms
            .fiat
            .currency
            .bytes()
            .all(|byte| byte.is_ascii_uppercase());
    let valid = valid_currency
        && valid_decimal(&terms.fiat.amount)
        && valid_positive_integer(&terms.bitcoin.amount)
        && terms.bitcoin.unit == "sat"
        && valid_versioned_identifier(&terms.payment_channel)
        && content.expires_at < terms.deadlines.fiat_pay_by
        && terms.deadlines.fiat_pay_by < terms.deadlines.fiat_confirm_by;
    if !valid {
        errors.push(ValidationError::new(
            ErrorCode::InvalidTerms,
            "root.content.terms",
            "swap terms do not satisfy pontmore/swap@1",
        ));
    }
    if !descriptor.networks.contains(&terms.bitcoin.network) {
        errors.push(ValidationError::new(
            ErrorCode::InvalidDescriptorBinding,
            "root.content.terms.bitcoin.network",
            "Bitcoin network is not supported by the accepted descriptor",
        ));
    }
}

fn root_descriptor_invalid(root_created_at: u64, descriptor: &DescriptorBinding) -> bool {
    !descriptor.selectable_at_root
        || !descriptor.authorities_validated
        || descriptor.created_at > root_created_at
        || root_created_at >= descriptor.expires_at
}

fn validate_descriptor_tags(
    root: &Event,
    descriptor: &DescriptorBinding,
    errors: &mut Vec<ValidationError>,
) {
    let escrow_events = marked_values(root, "e", "escrow-version");
    let escrow_addresses = marked_values(root, "a", "escrow");
    if escrow_events.as_slice() != [descriptor.event_id.as_str()]
        || escrow_addresses.as_slice() != [descriptor.coordinate.as_str()]
        || !valid_hex_32(&descriptor.event_id)
        || !valid_descriptor_coordinate(&descriptor.coordinate)
    {
        errors.push(ValidationError::new(
            ErrorCode::InvalidDescriptorBinding,
            "root.tags",
            "root must bind exactly the supplied descriptor event and coordinate",
        ));
    }
}

fn validate_roles(root: &Event, errors: &mut Vec<ValidationError>) -> Option<Roles> {
    let mut by_role: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for tag in root.tags.iter() {
        let cells = tag.as_slice();
        if cells.first().is_some_and(|value| value == "p") && cells.len() >= 4 {
            by_role
                .entry(cells[3].as_str())
                .or_default()
                .push(cells[1].as_str());
        }
    }
    let required = [
        "swap/agent",
        "swap/customer",
        "core/escrow",
        "core/resolver",
    ];
    let valid_counts = required
        .iter()
        .all(|role| by_role.get(role).is_some_and(|values| values.len() == 1));
    if !valid_counts || by_role.keys().any(|role| !required.contains(role)) {
        errors.push(ValidationError::new(
            ErrorCode::InvalidRoles,
            "root.tags",
            "swap root requires exactly one binding for each defined role",
        ));
        return None;
    }
    let values: BTreeSet<&str> = required
        .iter()
        .map(|role| by_role[role].first().copied().expect("validated role"))
        .collect();
    if values.len() != required.len() || values.iter().any(|value| !valid_hex_32(value)) {
        errors.push(ValidationError::new(
            ErrorCode::InvalidRoles,
            "root.tags",
            "pontmore/swap@1 does not permit one pubkey to hold multiple roles",
        ));
        return None;
    }
    let proposer = root.pubkey.to_hex();
    let agent = by_role["swap/agent"][0].to_owned();
    let customer = by_role["swap/customer"][0].to_owned();
    if proposer != agent && proposer != customer {
        errors.push(ValidationError::new(
            ErrorCode::UnauthorizedSigner,
            "root.pubkey",
            "root signer must be an application participant",
        ));
        return None;
    }
    Some(Roles {
        agent,
        customer,
        escrow: by_role["core/escrow"][0].to_owned(),
        resolver: by_role["core/resolver"][0].to_owned(),
        proposer,
    })
}

fn parse_action(
    event: &Event,
    root_id: &str,
    roles: &Roles,
) -> Result<ParsedAction, (ErrorCode, String)> {
    if event.verify().is_err() {
        return Err((
            ErrorCode::InvalidEvent,
            "invalid Nostr event ID or signature".into(),
        ));
    }
    if event.kind != ACTION_KIND {
        return Err((
            ErrorCode::InvalidEventKind,
            "coordination action kind must be 7301".into(),
        ));
    }
    let roots = marked_values(event, "e", "root");
    let predecessors = marked_values(event, "e", "prev");
    if roots.as_slice() != [root_id]
        || predecessors.len() != 1
        || !valid_hex_32(predecessors[0])
        || !valid_action_role_hints(event, roles)
    {
        return Err((
            ErrorCode::InvalidActionTags,
            "action must contain exactly one matching root and one predecessor reference".into(),
        ));
    }
    let content = serde_json::from_str::<ActionContent>(&event.content).map_err(|error| {
        (
            ErrorCode::InvalidContent,
            format!("invalid action content: {error}"),
        )
    })?;
    if content.version != 2 {
        return Err((
            ErrorCode::UnsupportedVersion,
            "action content version must be 2".into(),
        ));
    }
    Ok(ParsedAction {
        event: event.clone(),
        content,
        predecessor: predecessors[0].to_owned(),
    })
}

fn valid_action_role_hints(event: &Event, roles: &Roles) -> bool {
    event.tags.iter().all(|tag| match tag.as_slice() {
        [kind, pubkey, _, role, ..] if kind == "p" => match role.as_str() {
            "swap/agent" => pubkey == &roles.agent,
            "swap/customer" => pubkey == &roles.customer,
            "core/escrow" => pubkey == &roles.escrow,
            "core/resolver" => pubkey == &roles.resolver,
            _ => false,
        },
        _ => true,
    })
}

fn apply_action(
    machine: &Machine,
    action: &ParsedAction,
    root: &RootContent,
    roles: &Roles,
    descriptor: &DescriptorBinding,
) -> Result<Machine, (ErrorCode, String)> {
    if machine.state.terminal() {
        return invalid(
            ErrorCode::ActionAfterTerminal,
            "no action is valid after a terminal outcome",
        );
    }
    let id = action.content.action.as_str();
    if machine.state == CoordinationState::Disputed && id != "core/resolve_dispute" {
        return invalid(
            ErrorCode::InvalidTransition,
            "only dispute resolution is valid while disputed",
        );
    }
    validate_action_data(id, action.content.data.as_ref())?;
    match id {
        "swap/fiat_sent" | "swap/fiat_confirmed" => apply_swap_action(machine, action, root, roles),
        "core/accept" | "core/decline" | "core/cancel" | "core/expire" | "core/secure" => {
            apply_lifecycle_action(machine, action, root, roles, descriptor)
        }
        "core/authorize_settlement" | "core/settle" | "core/authorize_refund" | "core/refund" => {
            apply_economic_action(machine, action, root, roles)
        }
        "core/open_dispute" | "core/resolve_dispute" => {
            apply_dispute_action(machine, action, roles, descriptor)
        }
        _ => invalid(
            ErrorCode::UnknownAction,
            "action is not defined by PIP-02 or pontmore/swap@1",
        ),
    }
}

fn apply_lifecycle_action(
    machine: &Machine,
    action: &ParsedAction,
    root: &RootContent,
    roles: &Roles,
    descriptor: &DescriptorBinding,
) -> Result<Machine, (ErrorCode, String)> {
    let actor = action.event.pubkey.to_hex();
    let time = action.event.created_at.as_secs();
    let mut next = machine.clone();
    match action.content.action.as_str() {
        "core/accept" => {
            require(
                machine.state == CoordinationState::Proposed,
                "accept requires proposed state",
            )?;
            require(
                actor == other_participant(roles),
                "accept requires the non-proposing participant",
            )?;
            require(
                time < root.expires_at,
                "accept occurred at or after root expiry",
            )?;
            require(
                descriptor.current_at_acceptance && time < descriptor.expires_at,
                "descriptor was not current and unexpired at acceptance",
            )?;
            next.state = CoordinationState::Accepted;
        }
        "core/decline" => {
            require(
                machine.state == CoordinationState::Proposed,
                "decline requires proposed state",
            )?;
            require(
                actor == other_participant(roles),
                "decline requires the non-proposing participant",
            )?;
            next.state = CoordinationState::Declined;
        }
        "core/cancel" => {
            let valid_actor = if machine.state == CoordinationState::Proposed {
                actor == roles.proposer
            } else {
                is_participant(&actor, roles) && machine.state == CoordinationState::Accepted
            };
            require(valid_actor, "cancel signer or state is not authorized")?;
            next.state = CoordinationState::Cancelled;
        }
        "core/expire" => {
            require(
                machine.state == CoordinationState::Proposed,
                "expire requires unaccepted state",
            )?;
            require(
                is_participant(&actor, roles),
                "expire requires an application participant",
            )?;
            require(
                time >= root.expires_at,
                "acceptance deadline has not elapsed",
            )?;
            next.state = CoordinationState::Expired;
        }
        "core/secure" => {
            authorize(&actor, &roles.escrow)?;
            require(
                machine.state == CoordinationState::Accepted,
                "secure requires accepted state",
            )?;
            next.state = CoordinationState::Secured;
            next.secured = true;
        }
        _ => unreachable!("lifecycle dispatcher only passes lifecycle actions"),
    }
    Ok(next)
}

fn apply_swap_action(
    machine: &Machine,
    action: &ParsedAction,
    root: &RootContent,
    roles: &Roles,
) -> Result<Machine, (ErrorCode, String)> {
    let actor = action.event.pubkey.to_hex();
    let time = action.event.created_at.as_secs();
    let mut next = machine.clone();
    match action.content.action.as_str() {
        "swap/fiat_sent" => {
            authorize(&actor, fiat_sender(root, roles))?;
            require(
                machine.state == CoordinationState::Secured,
                "fiat_sent requires secured state",
            )?;
            require(
                time <= root.terms.deadlines.fiat_pay_by,
                "fiat_sent is after fiat_pay_by",
            )?;
            next.payment_reference = payment_reference(action.content.data.as_ref());
            next.state = CoordinationState::FiatSent;
        }
        "swap/fiat_confirmed" => {
            authorize(&actor, fiat_receiver(root, roles))?;
            require(
                machine.state == CoordinationState::FiatSent,
                "fiat_confirmed requires fiat_sent",
            )?;
            require(
                time <= root.terms.deadlines.fiat_confirm_by,
                "fiat_confirmed is after deadline",
            )?;
            require(
                payment_reference(action.content.data.as_ref()) == machine.payment_reference,
                "payment_reference does not match fiat_sent",
            )?;
            next.state = CoordinationState::FiatConfirmed;
        }
        _ => unreachable!("profile dispatcher only passes swap actions"),
    }
    Ok(next)
}

fn apply_economic_action(
    machine: &Machine,
    action: &ParsedAction,
    root: &RootContent,
    roles: &Roles,
) -> Result<Machine, (ErrorCode, String)> {
    let actor = action.event.pubkey.to_hex();
    let time = action.event.created_at.as_secs();
    let mut next = machine.clone();
    match action.content.action.as_str() {
        "core/authorize_settlement" => {
            authorize(&actor, fiat_receiver(root, roles))?;
            require(
                machine.state == CoordinationState::FiatConfirmed,
                "settlement authorization requires fiat confirmation",
            )?;
            next.state = CoordinationState::SettlementAuthorized;
        }
        "core/settle" => {
            authorize(&actor, &roles.escrow)?;
            require(
                machine.state == CoordinationState::SettlementAuthorized,
                "settle requires settlement authorization",
            )?;
            next.state = CoordinationState::Settled;
        }
        "core/authorize_refund" => {
            authorize(&actor, bitcoin_provider(root, roles))?;
            require(
                machine.state == CoordinationState::Secured,
                "refund authorization requires secured state",
            )?;
            require(
                time >= root.terms.deadlines.fiat_pay_by,
                "fiat_pay_by has not elapsed",
            )?;
            next.state = CoordinationState::RefundAuthorized;
        }
        "core/refund" => {
            authorize(&actor, &roles.escrow)?;
            require(
                machine.state == CoordinationState::RefundAuthorized,
                "refund requires refund authorization",
            )?;
            next.state = CoordinationState::Refunded;
        }
        _ => unreachable!("economic dispatcher only passes economic actions"),
    }
    Ok(next)
}

fn apply_dispute_action(
    machine: &Machine,
    action: &ParsedAction,
    roles: &Roles,
    descriptor: &DescriptorBinding,
) -> Result<Machine, (ErrorCode, String)> {
    let actor = action.event.pubkey.to_hex();
    let mut next = machine.clone();
    match action.content.action.as_str() {
        "core/open_dispute" => {
            require(
                is_participant(&actor, roles),
                "dispute requires an application participant",
            )?;
            require(
                machine.state != CoordinationState::Proposed,
                "dispute requires acceptance",
            )?;
            next.resume_state = Some(machine.state);
            next.state = CoordinationState::Disputed;
        }
        "core/resolve_dispute" => {
            authorize(&actor, &roles.resolver)?;
            let effect = resolution_effect(action.content.data.as_ref())?;
            require(
                descriptor.supported_resolution_effects.contains(&effect),
                "resolution effect is unsupported by the accepted service",
            )?;
            next.state = match effect {
                ResolutionEffect::Resume => machine.resume_state.ok_or_else(|| {
                    (
                        ErrorCode::InvalidTransition,
                        "missing pre-dispute state".into(),
                    )
                })?,
                ResolutionEffect::AuthorizeSettlement => {
                    require(
                        machine.secured,
                        "settlement resolution requires prior security",
                    )?;
                    CoordinationState::SettlementAuthorized
                }
                ResolutionEffect::AuthorizeRefund => {
                    require(machine.secured, "refund resolution requires prior security")?;
                    CoordinationState::RefundAuthorized
                }
                ResolutionEffect::Cancel => CoordinationState::Cancelled,
            };
            next.resume_state = None;
        }
        _ => unreachable!("dispute dispatcher only passes dispute actions"),
    }
    Ok(next)
}

fn validate_action_data(action: &str, data: Option<&Value>) -> Result<(), (ErrorCode, String)> {
    let object = match data {
        Some(Value::Object(object)) => object,
        None => {
            return if matches!(
                action,
                "swap/fiat_sent" | "swap/fiat_confirmed" | "core/resolve_dispute"
            ) {
                invalid(
                    ErrorCode::InvalidActionData,
                    "action requires a data object",
                )
            } else {
                Ok(())
            };
        }
        Some(_) => {
            return invalid(
                ErrorCode::InvalidActionData,
                "action data must be an object",
            );
        }
    };
    let allowed: &[&str] = match action {
        "swap/fiat_sent" | "swap/fiat_confirmed" => &["payment_reference"],
        "core/open_dispute" => &["class", "evidence"],
        "core/resolve_dispute" => &["policy", "effect", "evidence"],
        value if value.starts_with("core/") => &["evidence"],
        _ => &[],
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return invalid(
            ErrorCode::InvalidActionData,
            "action data contains an unsupported field",
        );
    }
    if matches!(action, "swap/fiat_sent" | "swap/fiat_confirmed") {
        let reference = object.get("payment_reference").and_then(Value::as_str);
        if reference.is_none_or(str::is_empty) {
            return invalid(
                ErrorCode::InvalidActionData,
                "payment_reference must be non-empty",
            );
        }
    }
    if action == "core/open_dispute"
        && let Some(class) = object.get("class").and_then(Value::as_str)
        && !matches!(
            class,
            "fiat_not_received"
                | "incorrect_fiat_amount"
                | "payment_reference_invalid"
                | "escrow_not_secured"
                | "bitcoin_not_released"
                | "conflicting_confirmation"
                | "timeout"
        )
    {
        return invalid(
            ErrorCode::InvalidActionData,
            "unsupported swap dispute class",
        );
    }
    if action == "core/resolve_dispute" {
        if object
            .get("policy")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return invalid(
                ErrorCode::InvalidActionData,
                "resolution policy must be non-empty",
            );
        }
        resolution_effect(data)?;
    }
    validate_evidence(object)
}

fn validate_evidence(object: &Map<String, Value>) -> Result<(), (ErrorCode, String)> {
    let Some(evidence) = object.get("evidence") else {
        return Ok(());
    };
    let Some(items) = evidence.as_array() else {
        return invalid(ErrorCode::InvalidActionData, "evidence must be an array");
    };
    for item in items {
        let Some(item) = item.as_object() else {
            return invalid(
                ErrorCode::InvalidActionData,
                "evidence item must be an object",
            );
        };
        let evidence_type = item.get("type").and_then(Value::as_str);
        let value = item.get("value").and_then(Value::as_str);
        let valid_value = match (evidence_type, value) {
            (Some("event"), Some(value)) => valid_hex_32(value),
            (Some("commitment"), Some(value)) => {
                value.split_once(':').is_some_and(|(algorithm, digest)| {
                    valid_versioned_identifier(algorithm) && !digest.is_empty()
                })
            }
            (Some("opaque"), Some(value)) => !value.is_empty(),
            _ => false,
        };
        if item.len() != 2 || !valid_value {
            return invalid(ErrorCode::InvalidActionData, "invalid evidence reference");
        }
    }
    Ok(())
}

fn resolution_effect(data: Option<&Value>) -> Result<ResolutionEffect, (ErrorCode, String)> {
    let effect = data
        .and_then(Value::as_object)
        .and_then(|object| object.get("effect"))
        .and_then(Value::as_str);
    match effect {
        Some("resume") => Ok(ResolutionEffect::Resume),
        Some("authorize_settlement") => Ok(ResolutionEffect::AuthorizeSettlement),
        Some("authorize_refund") => Ok(ResolutionEffect::AuthorizeRefund),
        Some("cancel") => Ok(ResolutionEffect::Cancel),
        _ => invalid(
            ErrorCode::InvalidActionData,
            "invalid dispute resolution effect",
        ),
    }
}

fn marked_values<'a>(event: &'a Event, kind: &str, marker: &str) -> Vec<&'a str> {
    event
        .tags
        .iter()
        .filter_map(|tag| match tag.as_slice() {
            [tag_kind, value, _, tag_marker, ..] if tag_kind == kind && tag_marker == marker => {
                Some(value.as_str())
            }
            _ => None,
        })
        .collect()
}

fn other_participant(roles: &Roles) -> String {
    if roles.proposer == roles.agent {
        roles.customer.clone()
    } else {
        roles.agent.clone()
    }
}

fn is_participant(pubkey: &str, roles: &Roles) -> bool {
    pubkey == roles.agent || pubkey == roles.customer
}

fn fiat_sender<'a>(root: &RootContent, roles: &'a Roles) -> &'a str {
    match root.terms.direction {
        Direction::FiatToBtc => &roles.customer,
        Direction::BtcToFiat => &roles.agent,
    }
}

fn fiat_receiver<'a>(root: &RootContent, roles: &'a Roles) -> &'a str {
    match root.terms.direction {
        Direction::FiatToBtc => &roles.agent,
        Direction::BtcToFiat => &roles.customer,
    }
}

fn bitcoin_provider<'a>(root: &RootContent, roles: &'a Roles) -> &'a str {
    fiat_receiver(root, roles)
}

fn payment_reference(data: Option<&Value>) -> Option<String> {
    data.and_then(Value::as_object)
        .and_then(|object| object.get("payment_reference"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn authorize(actor: &str, expected: &str) -> Result<(), (ErrorCode, String)> {
    if actor == expected {
        Ok(())
    } else {
        invalid(
            ErrorCode::UnauthorizedSigner,
            "action signer is not authorized",
        )
    }
}

fn require(condition: bool, message: &str) -> Result<(), (ErrorCode, String)> {
    if condition {
        Ok(())
    } else {
        invalid(ErrorCode::InvalidTransition, message)
    }
}

fn invalid<T>(code: ErrorCode, message: &str) -> Result<T, (ErrorCode, String)> {
    Err((code, message.into()))
}

fn reject(
    rejections: &mut Vec<EventRejection>,
    event_id: String,
    code: ErrorCode,
    message: impl Into<String>,
) {
    rejections.push(EventRejection {
        event_id,
        code,
        message: message.into(),
    });
}

fn valid_sha256_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

fn valid_hex_32(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_descriptor_coordinate(value: &str) -> bool {
    let mut parts = value.splitn(3, ':');
    matches!(parts.next(), Some("30361"))
        && parts.next().is_some_and(valid_hex_32)
        && parts
            .next()
            .is_some_and(|identifier| !identifier.is_empty())
}

fn valid_versioned_identifier(value: &str) -> bool {
    let Some((name, version)) = value.rsplit_once('@') else {
        return false;
    };
    !name.is_empty() && version.parse::<u64>().is_ok_and(|version| version > 0)
}

fn valid_positive_integer(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.bytes().any(|byte| byte != b'0')
}

fn valid_decimal(value: &str) -> bool {
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction
            .is_some_and(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return false;
    }
    value
        .bytes()
        .any(|byte| byte.is_ascii_digit() && byte != b'0')
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::prelude::{EventBuilder, FinalizeEvent, Keys, Tag, Timestamp};
    use serde_json::json;

    struct Actors {
        agent: Keys,
        customer: Keys,
        escrow: Keys,
        resolver: Keys,
    }

    impl Actors {
        fn fixed() -> Self {
            Self {
                agent: key(1),
                customer: key(2),
                escrow: key(3),
                resolver: key(4),
            }
        }
    }

    fn key(value: u8) -> Keys {
        Keys::parse(&format!("{value:064x}")).expect("fixed test secret key")
    }

    fn descriptor() -> DescriptorBinding {
        DescriptorBinding {
            event_id: "aa".repeat(32),
            coordinate: format!("30361:{}:cashu-main", key(5).public_key().to_hex()),
            created_at: 90,
            selectable_at_root: true,
            current_at_acceptance: true,
            expires_at: 1_000,
            networks: BTreeSet::from(["cashu".into()]),
            authorities_validated: true,
            supported_resolution_effects: BTreeSet::from([
                ResolutionEffect::Resume,
                ResolutionEffect::AuthorizeSettlement,
                ResolutionEffect::AuthorizeRefund,
                ResolutionEffect::Cancel,
            ]),
        }
    }

    fn root(actors: &Actors, descriptor: &DescriptorBinding) -> Event {
        root_for_direction(actors, descriptor, "fiat_to_btc")
    }

    fn root_for_direction(
        actors: &Actors,
        descriptor: &DescriptorBinding,
        direction: &str,
    ) -> Event {
        let tags = [
            role_tag(&actors.agent, "swap/agent"),
            role_tag(&actors.customer, "swap/customer"),
            role_tag(&actors.escrow, "core/escrow"),
            role_tag(&actors.resolver, "core/resolver"),
            Tag::custom("e", [descriptor.event_id.as_str(), "", "escrow-version"]),
            Tag::custom("a", [descriptor.coordinate.as_str(), "", "escrow"]),
        ];
        let content = json!({
            "version": 2,
            "profile": SWAP_PROFILE,
            "terms": {
                "direction": direction,
                "fiat": { "currency": "KES", "amount": "1000" },
                "bitcoin": { "amount": "60000", "unit": "sat", "network": "cashu" },
                "payment_channel": "mpesa-ke-kes@1",
                "deadlines": { "fiat_pay_by": 300, "fiat_confirm_by": 400 }
            },
            "expires_at": 200,
            "commitments": {
                "private_terms": {
                    "algorithm": "sha256-bytes@1",
                    "digest": format!("sha256:{}", "ab".repeat(32))
                }
            }
        });
        signed_event(ROOT_KIND, &content, tags, 100, &actors.agent)
    }

    fn role_tag(keys: &Keys, role: &str) -> Tag {
        Tag::custom(
            "p",
            [keys.public_key().to_hex(), String::new(), role.into()],
        )
    }

    fn action(
        root: &Event,
        previous: &Event,
        signer: &Keys,
        name: &str,
        data: Option<Value>,
        created_at: u64,
    ) -> Event {
        let mut content =
            Map::from_iter([("version".into(), json!(2)), ("action".into(), json!(name))]);
        if let Some(data) = data {
            content.insert("data".into(), data);
        }
        let root_id = root.id.to_hex();
        let previous_id = previous.id.to_hex();
        let tags = [
            Tag::custom("e", [root_id.as_str(), "", "root"]),
            Tag::custom("e", [previous_id.as_str(), "", "prev"]),
        ];
        signed_event(
            ACTION_KIND,
            &Value::Object(content),
            tags,
            created_at,
            signer,
        )
    }

    fn signed_event<const N: usize>(
        kind: Kind,
        content: &Value,
        tags: [Tag; N],
        created_at: u64,
        signer: &Keys,
    ) -> Event {
        EventBuilder::new(kind, content.to_string())
            .tags(tags)
            .custom_created_at(Timestamp::from_secs(created_at))
            .finalize(signer)
            .expect("sign test event")
    }

    fn settlement_history(actors: &Actors, root: &Event) -> Vec<Event> {
        let accept = action(root, root, &actors.customer, "core/accept", None, 110);
        let secure = action(root, &accept, &actors.escrow, "core/secure", None, 120);
        let sent = action(
            root,
            &secure,
            &actors.customer,
            "swap/fiat_sent",
            Some(json!({ "payment_reference": "commitment:payment-1" })),
            130,
        );
        let confirmed = action(
            root,
            &sent,
            &actors.agent,
            "swap/fiat_confirmed",
            Some(json!({ "payment_reference": "commitment:payment-1" })),
            140,
        );
        let authorized = action(
            root,
            &confirmed,
            &actors.agent,
            "core/authorize_settlement",
            None,
            150,
        );
        let settled = action(root, &authorized, &actors.escrow, "core/settle", None, 160);
        vec![accept, secure, sent, confirmed, authorized, settled]
    }

    #[test]
    fn reconstructs_settlement_independent_of_input_order() {
        let actors = Actors::fixed();
        let descriptor = descriptor();
        let root = root(&actors, &descriptor);
        let mut actions = settlement_history(&actors, &root);
        actions.reverse();

        let result = validate_swap_coordination(&root, &actions, &descriptor).expect("valid root");
        assert_eq!(result.state, CoordinationState::Settled);
        assert_eq!(result.applied.len(), 6);
        assert!(result.rejections.is_empty());
    }

    #[test]
    fn derives_reversed_economic_roles_for_btc_to_fiat() {
        let actors = Actors::fixed();
        let descriptor = descriptor();
        let root = root_for_direction(&actors, &descriptor, "btc_to_fiat");
        let accept = action(&root, &root, &actors.customer, "core/accept", None, 110);
        let secure = action(&root, &accept, &actors.escrow, "core/secure", None, 120);
        let sent = action(
            &root,
            &secure,
            &actors.agent,
            "swap/fiat_sent",
            Some(json!({ "payment_reference": "commitment:payment-2" })),
            130,
        );
        let confirmed = action(
            &root,
            &sent,
            &actors.customer,
            "swap/fiat_confirmed",
            Some(json!({ "payment_reference": "commitment:payment-2" })),
            140,
        );
        let authorized = action(
            &root,
            &confirmed,
            &actors.customer,
            "core/authorize_settlement",
            None,
            150,
        );

        let result = validate_swap_coordination(
            &root,
            &[authorized, confirmed, sent, secure, accept],
            &descriptor,
        )
        .expect("valid root");
        assert_eq!(result.state, CoordinationState::SettlementAuthorized);
        assert!(result.rejections.is_empty());
    }

    #[test]
    fn detects_two_otherwise_valid_siblings_as_a_fork() {
        let actors = Actors::fixed();
        let descriptor = descriptor();
        let root = root(&actors, &descriptor);
        let first = action(&root, &root, &actors.customer, "core/accept", None, 110);
        let second = action(&root, &root, &actors.customer, "core/accept", None, 111);

        let result =
            validate_swap_coordination(&root, &[second, first], &descriptor).expect("valid root");
        assert_eq!(result.state, CoordinationState::Forked);
        assert_eq!(result.fork.expect("fork evidence").siblings.len(), 2);
    }

    #[test]
    fn rejects_unauthorized_economic_action_without_advancing_tip() {
        let actors = Actors::fixed();
        let descriptor = descriptor();
        let root = root(&actors, &descriptor);
        let accept = action(&root, &root, &actors.customer, "core/accept", None, 110);
        let secure = action(&root, &accept, &actors.agent, "core/secure", None, 120);

        let result = validate_swap_coordination(&root, &[secure, accept.clone()], &descriptor)
            .expect("valid root");
        assert_eq!(result.state, CoordinationState::Accepted);
        assert_eq!(result.tip, accept.id.to_hex());
        assert_eq!(result.rejections[0].code, ErrorCode::UnauthorizedSigner);
    }

    #[test]
    fn reconstructs_no_payment_refund() {
        let actors = Actors::fixed();
        let descriptor = descriptor();
        let root = root(&actors, &descriptor);
        let accept = action(&root, &root, &actors.customer, "core/accept", None, 110);
        let secure = action(&root, &accept, &actors.escrow, "core/secure", None, 120);
        let authorize = action(
            &root,
            &secure,
            &actors.agent,
            "core/authorize_refund",
            None,
            300,
        );
        let refund = action(&root, &authorize, &actors.escrow, "core/refund", None, 310);

        let result =
            validate_swap_coordination(&root, &[refund, authorize, secure, accept], &descriptor)
                .expect("valid root");
        assert_eq!(result.state, CoordinationState::Refunded);
    }

    #[test]
    fn dispute_resolution_does_not_itself_settle() {
        let actors = Actors::fixed();
        let descriptor = descriptor();
        let root = root(&actors, &descriptor);
        let accept = action(&root, &root, &actors.customer, "core/accept", None, 110);
        let secure = action(&root, &accept, &actors.escrow, "core/secure", None, 120);
        let dispute = action(
            &root,
            &secure,
            &actors.customer,
            "core/open_dispute",
            Some(json!({ "class": "fiat_not_received" })),
            130,
        );
        let resolve = action(
            &root,
            &dispute,
            &actors.resolver,
            "core/resolve_dispute",
            Some(json!({ "policy": "escrow-policy@1", "effect": "authorize_refund" })),
            140,
        );

        let result =
            validate_swap_coordination(&root, &[resolve, dispute, secure, accept], &descriptor)
                .expect("valid root");
        assert_eq!(result.state, CoordinationState::RefundAuthorized);
    }

    #[test]
    fn rejects_tampered_signed_action() {
        let actors = Actors::fixed();
        let descriptor = descriptor();
        let root = root(&actors, &descriptor);
        let mut accept = action(&root, &root, &actors.customer, "core/accept", None, 110);
        accept.content.push(' ');

        let result = validate_swap_coordination(&root, &[accept], &descriptor).expect("valid root");
        assert_eq!(result.state, CoordinationState::Proposed);
        assert_eq!(result.rejections[0].code, ErrorCode::InvalidEvent);
    }

    #[test]
    fn rejects_every_copy_of_a_duplicate_event_id() {
        let actors = Actors::fixed();
        let descriptor = descriptor();
        let root = root(&actors, &descriptor);
        let accept = action(&root, &root, &actors.customer, "core/accept", None, 110);

        let result = validate_swap_coordination(&root, &[accept.clone(), accept], &descriptor)
            .expect("valid root");
        assert_eq!(result.state, CoordinationState::Proposed);
        assert_eq!(result.rejections.len(), 1);
        assert_eq!(result.rejections[0].code, ErrorCode::DuplicateEvent);
    }

    #[test]
    fn rejects_a_descriptor_expired_when_the_root_was_created() {
        let actors = Actors::fixed();
        let mut descriptor = descriptor();
        descriptor.expires_at = 100;
        let root = root(&actors, &descriptor);

        let errors = validate_swap_coordination(&root, &[], &descriptor).expect_err("invalid root");
        assert!(
            errors
                .iter()
                .any(|error| error.code == ErrorCode::InvalidDescriptorBinding)
        );
    }
}
