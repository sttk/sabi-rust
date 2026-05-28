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
            rollback: TxnFailureRollback::NoneByNotRolledBack,
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
        use TxnFailureCause::*;
        use TxnFailureRecovery::*;
        use TxnFailureRollback::*;

        match (&self.cause, &self.rollback) {
            (NoneByUncommitted, NoneByNotRolledBack) => InvestigateBecauseImpossible,
            (NoneByUncommitted, NoneByRolledBack) => RerunLogicAndCommit,
            (NoneByUncommitted, RollbackFailure(_)) => ResolveCauseAndInconsistency,

            (NoneByCommitted, NoneByNotRolledBack) => NoActionRequired,
            (NoneByCommitted, NoneByRolledBack) => InvestigateBecauseImpossible,
            (NoneByCommitted, RollbackFailure(_)) => InvestigateBecauseImpossible,

            (LogicFailure(_), NoneByNotRolledBack) => InvestigateBecauseImpossible,
            (LogicFailure(_), NoneByRolledBack) => ResolveCauseThenRerunLogicAndCommit,
            (LogicFailure(_), RollbackFailure(_)) => ResolveCauseAndInconsistency,

            (CommitFailure(_), NoneByNotRolledBack) => InvestigateBecauseImpossible,
            (CommitFailure(_), NoneByRolledBack) => ResolveCauseThenRerunLogicAndCommit,
            (CommitFailure(_), RollbackFailure(_)) => ResolveCauseAndInconsistency,

            (PostCommitFailure(_), NoneByNotRolledBack) => ResolveCauseThenRerunPostCommit,
            (PostCommitFailure(_), NoneByRolledBack) => InvestigateBecauseImpossible,
            (PostCommitFailure(_), RollbackFailure(_)) => InvestigateBecauseImpossible,
        }
    }

    /// Determines the suggested recovery action to achieve a rolled-back state for this connection.
    pub fn recovery_for_rollback(&self) -> TxnFailureRecovery {
        use TxnFailureCause::*;
        use TxnFailureRecovery::*;
        use TxnFailureRollback::*;

        match (&self.cause, &self.rollback) {
            (NoneByUncommitted, NoneByNotRolledBack) => InvestigateBecauseImpossible,
            (NoneByUncommitted, NoneByRolledBack) => NoActionRequired,
            (NoneByUncommitted, RollbackFailure(_)) => ResolveCauseAndInconsistency,

            (NoneByCommitted, NoneByNotRolledBack) => ManualRollbackRequired,
            (NoneByCommitted, NoneByRolledBack) => InvestigateBecauseImpossible,
            (NoneByCommitted, RollbackFailure(_)) => InvestigateBecauseImpossible,

            (LogicFailure(_), NoneByNotRolledBack) => InvestigateBecauseImpossible,
            (LogicFailure(_), NoneByRolledBack) => NoActionRequired,
            (LogicFailure(_), RollbackFailure(_)) => ResolveCauseAndInconsistency,

            (CommitFailure(_), NoneByNotRolledBack) => InvestigateBecauseImpossible,
            (CommitFailure(_), NoneByRolledBack) => NoActionRequired,
            (CommitFailure(_), RollbackFailure(_)) => ResolveCauseAndInconsistency,

            (PostCommitFailure(_), NoneByNotRolledBack) => ManualRollbackRequired,
            (PostCommitFailure(_), NoneByRolledBack) => InvestigateBecauseImpossible,
            (PostCommitFailure(_), RollbackFailure(_)) => InvestigateBecauseImpossible,
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use TxnFailureCause::*;
    use TxnFailureRecovery::*;
    use TxnFailureRollback::*;

    // impossible case
    #[test]
    fn test_cause_is_none_by_uncommitted_and_rollback_is_none_by_not_rolled_back() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: NoneByUncommitted,
            rollback: NoneByNotRolledBack,
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), InvestigateBecauseImpossible);
        assert_eq!(report.recovery_for_rollback(), InvestigateBecauseImpossible);
    }

    #[test]
    fn test_cause_is_none_by_uncommitted_and_rollback_is_none_by_rolled_back() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: NoneByUncommitted,
            rollback: NoneByRolledBack,
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), RerunLogicAndCommit);
        assert_eq!(report.recovery_for_rollback(), NoActionRequired);
    }

    #[test]
    fn test_cause_is_none_by_uncommitted_and_rollback_is_rollback_failure() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: NoneByUncommitted,
            rollback: RollbackFailure(errs::Err::new("fail")),
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), ResolveCauseAndInconsistency);
        assert_eq!(report.recovery_for_rollback(), ResolveCauseAndInconsistency);
    }

    #[test]
    fn test_cause_is_none_by_committed_and_rollback_is_none_by_not_rolled_back() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: NoneByCommitted,
            rollback: NoneByNotRolledBack,
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), NoActionRequired);
        assert_eq!(report.recovery_for_rollback(), ManualRollbackRequired);
    }

    // impossible case
    #[test]
    fn test_cause_is_none_by_committed_and_rollback_is_none_by_rolled_back() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::NoneByCommitted,
            rollback: TxnFailureRollback::NoneByRolledBack,
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), InvestigateBecauseImpossible);
        assert_eq!(report.recovery_for_rollback(), InvestigateBecauseImpossible);
    }

    // impossible case
    #[test]
    fn test_cause_is_none_by_committed_and_rollback_is_rollback_failure() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::NoneByCommitted,
            rollback: TxnFailureRollback::RollbackFailure(errs::Err::new("fail")),
        };

        assert!(!report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), InvestigateBecauseImpossible);
        assert_eq!(report.recovery_for_rollback(), InvestigateBecauseImpossible);
    }

    #[test]
    fn test_cause_is_logic_failure_and_rollback_is_none_by_not_rolled_back() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: LogicFailure(errs::Err::new("fail")),
            rollback: NoneByNotRolledBack,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), InvestigateBecauseImpossible);
        assert_eq!(report.recovery_for_rollback(), InvestigateBecauseImpossible);
    }

    #[test]
    fn test_cause_is_run_failure_and_rollback_is_none_by_rolled_back() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: LogicFailure(errs::Err::new("fail")),
            rollback: NoneByRolledBack,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            ResolveCauseThenRerunLogicAndCommit
        );
        assert_eq!(report.recovery_for_rollback(), NoActionRequired);
    }

    #[test]
    fn test_cause_is_run_failure_and_rollback_is_rollback_failure() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: LogicFailure(errs::Err::new("fail")),
            rollback: RollbackFailure(errs::Err::new("fail")),
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), ResolveCauseAndInconsistency);
        assert_eq!(report.recovery_for_rollback(), ResolveCauseAndInconsistency);
    }

    #[test]
    fn test_cause_is_commit_failure_and_rollback_is_none_by_not_rolled_back() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: CommitFailure(errs::Err::new("fail")),
            rollback: NoneByNotRolledBack,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), InvestigateBecauseImpossible);
        assert_eq!(report.recovery_for_rollback(), InvestigateBecauseImpossible);
    }

    #[test]
    fn test_cause_is_commit_failure_and_rollback_is_none_by_rolled_back() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: CommitFailure(errs::Err::new("fail")),
            rollback: NoneByRolledBack,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            ResolveCauseThenRerunLogicAndCommit
        );
        assert_eq!(report.recovery_for_rollback(), NoActionRequired);
    }

    #[test]
    fn test_cause_is_commit_failure_and_rollback_is_rollback_failure() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::CommitFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::RollbackFailure(errs::Err::new("fail")),
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), ResolveCauseAndInconsistency);
        assert_eq!(report.recovery_for_rollback(), ResolveCauseAndInconsistency);
    }

    #[test]
    fn test_cause_is_post_commit_failure_and_rollback_is_none_by_not_rolled_back() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: PostCommitFailure(errs::Err::new("fail")),
            rollback: NoneByNotRolledBack,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(
            report.recovery_for_commit(),
            ResolveCauseThenRerunPostCommit
        );
        assert_eq!(report.recovery_for_rollback(), ManualRollbackRequired);
    }

    #[test]
    fn test_cause_is_post_commit_failure_and_rollback_is_none_by_rolled_back() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: PostCommitFailure(errs::Err::new("fail")),
            rollback: NoneByRolledBack,
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), InvestigateBecauseImpossible);
        assert_eq!(report.recovery_for_rollback(), InvestigateBecauseImpossible);
    }

    #[test]
    fn test_cause_is_post_commit_failure_and_rollback_is_rollback_failure() {
        let report = TxnFailureReport {
            data_conn_name: "foo".into(),
            data_conn_type: "A::B::C",
            cause: TxnFailureCause::PostCommitFailure(errs::Err::new("fail")),
            rollback: TxnFailureRollback::RollbackFailure(errs::Err::new("fail")),
        };

        assert!(report.is_cause_of_failure());
        assert_eq!(report.recovery_for_commit(), InvestigateBecauseImpossible);
        assert_eq!(report.recovery_for_rollback(), InvestigateBecauseImpossible);
    }
}
