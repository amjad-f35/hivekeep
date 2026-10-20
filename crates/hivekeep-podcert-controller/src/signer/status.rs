//! Request status: whether a request is answered, and the answers.

use chrono::{DateTime, Utc};
use hivekeep_identity::pod_cert::IssuedPodCertificate;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};

use crate::pod_certificate::{
    PodCertificateRequest, CONDITION_DENIED, CONDITION_FAILED, CONDITION_ISSUED,
};

/// Whether the request already carries a final condition (issued, denied
/// or failed), so there is nothing left to do.
pub fn is_answered(pcr: &PodCertificateRequest) -> bool {
    pcr.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|conds| {
            conds.iter().any(|c| {
                matches!(
                    c.type_.as_str(),
                    CONDITION_ISSUED | CONDITION_DENIED | CONDITION_FAILED
                )
            })
        })
}

/// `pcr` with `issued` recorded as its answer.
pub fn with_issued(
    pcr: &PodCertificateRequest,
    issued: &IssuedPodCertificate,
    now: DateTime<Utc>,
) -> PodCertificateRequest {
    let mut pcr = pcr.clone();
    let status = pcr.status.get_or_insert_default();
    status.conditions = Some(vec![condition(CONDITION_ISSUED, "Reason", "Issued", now)]);
    status.certificate_chain = Some(issued.cert_chain_pem.clone());
    status.not_before = Some(time(issued.not_before.into()));
    status.begin_refresh_at = Some(time(issued.begin_refresh_at.into()));
    status.not_after = Some(time(issued.not_after.into()));
    pcr
}

/// `pcr` denied for `reason`.
pub fn with_denied(
    pcr: &PodCertificateRequest,
    reason: &str,
    message: &str,
    now: DateTime<Utc>,
) -> PodCertificateRequest {
    let mut pcr = pcr.clone();
    pcr.status.get_or_insert_default().conditions =
        Some(vec![condition(CONDITION_DENIED, reason, message, now)]);
    pcr
}

fn condition(type_: &str, reason: &str, message: &str, now: DateTime<Utc>) -> Condition {
    Condition {
        type_: type_.into(),
        status: "True".into(),
        reason: reason.into(),
        message: message.into(),
        last_transition_time: time(now),
        observed_generation: None,
    }
}

/// A Kubernetes timestamp, at the second precision the API keeps.
pub fn time(t: DateTime<Utc>) -> Time {
    Time(
        k8s_openapi::jiff::Timestamp::from_second(t.timestamp())
            .unwrap_or(k8s_openapi::jiff::Timestamp::UNIX_EPOCH),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::start_time;

    #[test]
    fn answered_requests_are_final() {
        let mut pcr = PodCertificateRequest::default();
        assert!(!is_answered(&pcr));
        for t in [CONDITION_ISSUED, CONDITION_DENIED, CONDITION_FAILED] {
            pcr.status.get_or_insert_default().conditions =
                Some(vec![condition(t, "r", "m", start_time())]);
            assert!(is_answered(&pcr), "{t}");
        }
        pcr.status.get_or_insert_default().conditions =
            Some(vec![condition("SomethingElse", "r", "m", start_time())]);
        assert!(!is_answered(&pcr));
    }

    #[test]
    fn denial_replaces_conditions_only() {
        let pcr = PodCertificateRequest::default();
        let denied = with_denied(&pcr, "Why", "because", start_time());
        let status = denied.status.unwrap();
        let cond = &status.conditions.unwrap()[0];
        assert_eq!(
            (
                cond.type_.as_str(),
                cond.status.as_str(),
                cond.reason.as_str()
            ),
            ("Denied", "True", "Why")
        );
        assert!(status.certificate_chain.is_none());
    }
}
