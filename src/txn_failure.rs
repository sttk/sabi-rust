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

    pub fn is_cause_of_failure(&self) -> bool {
        !matches!(
            self.cause,
            TxnFailureCause::NoneByCommitted | TxnFailureCause::NoneByUncommitted
        )
    }

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

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test;
