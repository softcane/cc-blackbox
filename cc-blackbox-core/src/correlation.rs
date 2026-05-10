#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxySessionCorrelation {
    pub session_id: String,
    pub cwd: String,
    pub started_at_epoch_secs: u64,
    pub last_activity_at_epoch_secs: u64,
    pub first_message_hash: String,
    pub request_count: u64,
    pub turn_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonlSessionCorrelation {
    pub jsonl_session_id: String,
    pub cwd: String,
    pub started_at_epoch_secs: u64,
    pub last_activity_at_epoch_secs: u64,
    pub first_message_hash: String,
    pub request_count: u64,
    pub turn_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyOnlyReason {
    MissingJsonl,
    NoMatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonlEnrichmentStatus {
    Matched {
        proxy_session_id: String,
        jsonl_session_id: String,
    },
    ProxyOnly {
        proxy_session_id: String,
        reason: ProxyOnlyReason,
    },
    Ambiguous {
        proxy_session_id: String,
        candidate_count: usize,
    },
}

pub fn correlate_jsonl_session(
    proxy: &ProxySessionCorrelation,
    candidates: &[JsonlSessionCorrelation],
) -> JsonlEnrichmentStatus {
    if candidates.is_empty() {
        return JsonlEnrichmentStatus::ProxyOnly {
            proxy_session_id: proxy.session_id.clone(),
            reason: ProxyOnlyReason::MissingJsonl,
        };
    }

    let matches = candidates
        .iter()
        .filter(|candidate| candidate_matches_proxy(proxy, candidate))
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [matched] => JsonlEnrichmentStatus::Matched {
            proxy_session_id: proxy.session_id.clone(),
            jsonl_session_id: matched.jsonl_session_id.clone(),
        },
        [] => JsonlEnrichmentStatus::ProxyOnly {
            proxy_session_id: proxy.session_id.clone(),
            reason: ProxyOnlyReason::NoMatch,
        },
        many => JsonlEnrichmentStatus::Ambiguous {
            proxy_session_id: proxy.session_id.clone(),
            candidate_count: many.len(),
        },
    }
}

fn normalize_cwd(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed == "/" {
        return trimmed.to_string();
    }
    trimmed.trim_end_matches('/').to_string()
}

fn windows_overlap_with_tolerance(
    a_start: u64,
    a_end: u64,
    b_start: u64,
    b_end: u64,
    tolerance_secs: u64,
) -> bool {
    let a_min = a_start.min(a_end).saturating_sub(tolerance_secs);
    let a_max = a_start.max(a_end).saturating_add(tolerance_secs);
    let b_min = b_start.min(b_end).saturating_sub(tolerance_secs);
    let b_max = b_start.max(b_end).saturating_add(tolerance_secs);
    a_min <= b_max && b_min <= a_max
}

fn nearby_count(proxy_count: u64, jsonl_count: u64) -> bool {
    proxy_count == 0 || jsonl_count == 0 || proxy_count.abs_diff(jsonl_count) <= 1
}

fn candidate_matches_proxy(
    proxy: &ProxySessionCorrelation,
    candidate: &JsonlSessionCorrelation,
) -> bool {
    normalize_cwd(&proxy.cwd) == normalize_cwd(&candidate.cwd)
        && proxy.first_message_hash == candidate.first_message_hash
        && windows_overlap_with_tolerance(
            proxy.started_at_epoch_secs,
            proxy.last_activity_at_epoch_secs,
            candidate.started_at_epoch_secs,
            candidate.last_activity_at_epoch_secs,
            300,
        )
        && nearby_count(proxy.request_count, candidate.request_count)
        && nearby_count(proxy.turn_count, candidate.turn_count)
}

#[cfg(test)]
mod tests {
    use super::{
        correlate_jsonl_session, JsonlEnrichmentStatus, JsonlSessionCorrelation, ProxyOnlyReason,
        ProxySessionCorrelation,
    };

    fn proxy_session() -> ProxySessionCorrelation {
        ProxySessionCorrelation {
            session_id: "session_proxy_1".to_string(),
            cwd: "/Users/pradeepsingh/code/cc-blackbox".to_string(),
            started_at_epoch_secs: 1_800,
            last_activity_at_epoch_secs: 2_100,
            first_message_hash: "firstmsg_alpha".to_string(),
            request_count: 4,
            turn_count: 4,
        }
    }

    fn jsonl_session() -> JsonlSessionCorrelation {
        JsonlSessionCorrelation {
            jsonl_session_id: "jsonl_1".to_string(),
            cwd: "/Users/pradeepsingh/code/cc-blackbox/".to_string(),
            started_at_epoch_secs: 1_790,
            last_activity_at_epoch_secs: 2_090,
            first_message_hash: "firstmsg_alpha".to_string(),
            request_count: 4,
            turn_count: 4,
        }
    }

    #[test]
    fn matches_proxy_and_jsonl_session_by_cwd_time_hash_and_cadence() {
        let status = correlate_jsonl_session(&proxy_session(), &[jsonl_session()]);

        assert_eq!(
            status,
            JsonlEnrichmentStatus::Matched {
                proxy_session_id: "session_proxy_1".to_string(),
                jsonl_session_id: "jsonl_1".to_string(),
            }
        );
    }

    #[test]
    fn parallel_sessions_in_same_cwd_match_distinct_first_message_hashes() {
        let mut proxy_beta = proxy_session();
        proxy_beta.session_id = "session_proxy_2".to_string();
        proxy_beta.first_message_hash = "firstmsg_beta".to_string();

        let jsonl_alpha = jsonl_session();
        let mut jsonl_beta = jsonl_session();
        jsonl_beta.jsonl_session_id = "jsonl_2".to_string();
        jsonl_beta.first_message_hash = "firstmsg_beta".to_string();

        assert_eq!(
            correlate_jsonl_session(&proxy_session(), &[jsonl_alpha.clone(), jsonl_beta.clone()]),
            JsonlEnrichmentStatus::Matched {
                proxy_session_id: "session_proxy_1".to_string(),
                jsonl_session_id: "jsonl_1".to_string(),
            }
        );
        assert_eq!(
            correlate_jsonl_session(&proxy_beta, &[jsonl_alpha, jsonl_beta]),
            JsonlEnrichmentStatus::Matched {
                proxy_session_id: "session_proxy_2".to_string(),
                jsonl_session_id: "jsonl_2".to_string(),
            }
        );
    }

    #[test]
    fn missing_mismatched_and_ambiguous_jsonl_return_explicit_statuses() {
        let proxy = proxy_session();
        assert_eq!(
            correlate_jsonl_session(&proxy, &[]),
            JsonlEnrichmentStatus::ProxyOnly {
                proxy_session_id: "session_proxy_1".to_string(),
                reason: ProxyOnlyReason::MissingJsonl,
            }
        );

        let mut late = jsonl_session();
        late.started_at_epoch_secs = 8_000;
        late.last_activity_at_epoch_secs = 8_200;
        assert_eq!(
            correlate_jsonl_session(&proxy, &[late]),
            JsonlEnrichmentStatus::ProxyOnly {
                proxy_session_id: "session_proxy_1".to_string(),
                reason: ProxyOnlyReason::NoMatch,
            }
        );

        let mut duplicate = jsonl_session();
        duplicate.jsonl_session_id = "jsonl_duplicate".to_string();
        assert_eq!(
            correlate_jsonl_session(&proxy, &[jsonl_session(), duplicate]),
            JsonlEnrichmentStatus::Ambiguous {
                proxy_session_id: "session_proxy_1".to_string(),
                candidate_count: 2,
            }
        );
    }
}
