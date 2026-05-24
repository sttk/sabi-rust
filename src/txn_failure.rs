// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{TxnFailureCause, TxnFailureRecovery, TxnFailureReport, TxnFailureRollback};

use std::sync::Arc;

impl TxnFailureReport {
    pub(crate) fn new(name: Arc<str>, typ: &'static str) -> Self {
        Self {
            data_conn_name: name,
            data_conn_type: typ,
            cause: TxnFailureCause::NoneByUncommitted,
            rollback: TxnFailureRollback::None,
        }
    }

    /// Returns `true` if this connection was a cause of the transaction failure.
    pub fn is_cause_of_failure(&self) -> bool {
        !matches!(
            self.cause,
            TxnFailureCause::NoneByCommitted | TxnFailureCause::NoneByUncommitted
        )
    }

    /// Determines the suggested recovery action for this connection to achieve a successful commit.
    pub fn recovery_for_commit(&self) -> TxnFailureRecovery {
        match (&self.cause, &self.rollback) {
            (TxnFailureCause::NoneByCommitted, _) => TxnFailureRecovery::NoActionRequired,
            (TxnFailureCause::PostCommitFailure(_), _) => TxnFailureRecovery::RetryPostCommit,
            (_, TxnFailureRollback::Success) => TxnFailureRecovery::RerunAndCommit,
            _ => TxnFailureRecovery::InvestigateAndRerunAndCommit,
        }
    }

    /// Determines the suggested recovery action to achieve a rolled-back state for this connection.
    pub fn recovery_for_rollback(&self) -> TxnFailureRecovery {
        match self.rollback {
            TxnFailureRollback::Success => TxnFailureRecovery::NoActionRequired,
            TxnFailureRollback::None => match self.cause {
                TxnFailureCause::NoneByCommitted => TxnFailureRecovery::CompensateRollback,
                _ => TxnFailureRecovery::InvestigateAndRollback,
            },
            _ => TxnFailureRecovery::InvestigateAndRollback,
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_cause_is_none_by_committed_and_rollback_is_none() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::NoneByCommitted,
            rollback: TxnFailureRollback::None,
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::NoActionRequired
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::CompensateRollback
        );
    }

    // impossible case
    #[test]
    fn test_cause_is_none_by_committed_and_rollback_is_success() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::NoneByCommitted,
            rollback: TxnFailureRollback::Success,
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::NoActionRequired
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::NoActionRequired
        );
    }

    // impossible case
    #[test]
    fn test_cause_is_none_by_committed_and_rollback_is_failure() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::NoneByCommitted,
            rollback: TxnFailureRollback::Failure(errs::Err::new("fail")),
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::NoActionRequired
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::InvestigateAndRollback
        );
    }

    // impossible case
    #[test]
    fn test_cause_is_none_by_uncommitted_and_rollback_is_none() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::NoneByUncommitted,
            rollback: TxnFailureRollback::None,
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::InvestigateAndRerunAndCommit
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::InvestigateAndRollback
        );
    }

    #[test]
    fn test_cause_is_none_by_uncommitted_and_rollback_is_success() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::NoneByUncommitted,
            rollback: TxnFailureRollback::Success,
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::RerunAndCommit,
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::NoActionRequired,
        );
    }

    #[test]
    fn test_cause_is_none_by_uncommitted_and_rollback_is_failure() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::NoneByUncommitted,
            rollback: TxnFailureRollback::Failure(errs::Err::new("fail")),
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::InvestigateAndRerunAndCommit
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::InvestigateAndRollback
        );
    }

    #[test]
    fn test_cause_is_run_failure_and_rollback_is_none() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::RunFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::None,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::InvestigateAndRerunAndCommit
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::InvestigateAndRollback
        );
    }

    #[test]
    fn test_cause_is_run_failure_and_rollback_is_success() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::RunFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::Success,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::RerunAndCommit,
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::NoActionRequired,
        );
    }

    #[test]
    fn test_cause_is_run_failure_and_rollback_is_failure() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::RunFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::Failure(errs::Err::new("fail")),
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::InvestigateAndRerunAndCommit
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::InvestigateAndRollback
        );
    }

    #[test]
    fn test_cause_is_commit_failure_and_rollback_is_none() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::CommitFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::None,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::InvestigateAndRerunAndCommit
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::InvestigateAndRollback
        );
    }

    #[test]
    fn test_cause_is_commit_failure_and_rollback_is_success() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::CommitFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::Success,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::RerunAndCommit,
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::NoActionRequired,
        );
    }

    #[test]
    fn test_cause_is_commit_failure_and_rollback_is_failure() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::CommitFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::Failure(errs::Err::new("fail")),
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::InvestigateAndRerunAndCommit
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::InvestigateAndRollback
        );
    }

    #[test]
    fn test_cause_is_post_commit_failure_and_rollback_is_none() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::PostCommitFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::None,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::RetryPostCommit,
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::InvestigateAndRollback
        );
    }

    #[test]
    fn test_cause_is_post_commit_failure_and_rollback_is_success() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::PostCommitFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::Success,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::RetryPostCommit,
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::NoActionRequired,
        );
    }

    #[test]
    fn test_cause_is_post_commit_failure_and_rollback_is_failure() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::PostCommitFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::Failure(errs::Err::new("fail")),
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            TxnFailureRecovery::RetryPostCommit,
        );
        assert_eq!(
            report.recovery_for_rollback(),
            TxnFailureRecovery::InvestigateAndRollback,
        );
    }
}
