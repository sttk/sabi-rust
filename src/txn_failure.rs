// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{TxnFailureCause, TxnFailureRecovery, TxnFailureReport, TxnFailureRollback};

use std::sync::Arc;

impl TxnFailureReport {
    pub(crate) fn new(name: Arc<str>, typ: &'static str, logic_result: &errs::Result<()>) -> Self {
        Self {
            data_conn_name: name,
            data_conn_type: typ,
            cause: match logic_result {
                Ok(_) => TxnFailureCause::NoneByUncommitted,
                Err(err) => TxnFailureCause::LogicFailure(err.clone()),
            },
            rollback: TxnFailureRollback::NoneByNotRolledBack,
        }
    }

    /// Returns whether this data connection is identified as the root cause of the
    /// transaction failure.
    ///
    /// # Returns
    ///
    /// `true` if the cause is [`TxnFailureCause::PreCommitFailure`],
    /// [`TxnFailureCause::CommitFailure`], or [`TxnFailureCause::PostCommitFailure`];
    /// `false` if it is [`TxnFailureCause::NoneByCommitted`],
    /// [`TxnFailureCause::NoneByUncommitted`], or [`TxnFailureCause::LogicFailure`].
    pub fn is_cause_of_failure(&self) -> bool {
        !matches!(
            self.cause,
            TxnFailureCause::NoneByCommitted
                | TxnFailureCause::NoneByUncommitted
                | TxnFailureCause::LogicFailure(_)
        )
    }

    /// Determines the recommended recovery action when committing a transaction failed.
    ///
    /// Inspects the combination of [`cause`](Self::cause) and [`rollback`](Self::rollback) to
    /// advise on whether logic can simply be rerun, root cause must be fixed first, or manual
    /// intervention is needed.
    ///
    /// # Returns
    ///
    /// The recommended [`TxnFailureRecovery`] action.
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

            (PreCommitFailure(_), NoneByNotRolledBack) => InvestigateBecauseImpossible,
            (PreCommitFailure(_), NoneByRolledBack) => ResolveCauseThenRerunLogicAndCommit,
            (PreCommitFailure(_), RollbackFailure(_)) => ResolveCauseAndInconsistency,

            (CommitFailure(_), NoneByNotRolledBack) => InvestigateBecauseImpossible,
            (CommitFailure(_), NoneByRolledBack) => ResolveCauseThenRerunLogicAndCommit,
            (CommitFailure(_), RollbackFailure(_)) => ResolveCauseAndInconsistency,

            (PostCommitFailure(_), NoneByNotRolledBack) => ResolveCauseThenRerunPostCommit,
            (PostCommitFailure(_), NoneByRolledBack) => InvestigateBecauseImpossible,
            (PostCommitFailure(_), RollbackFailure(_)) => InvestigateBecauseImpossible,
        }
    }

    /// Determines the recommended recovery action when rolling back a transaction failed.
    ///
    /// Inspects the combination of [`cause`](Self::cause) and [`rollback`](Self::rollback) to
    /// determine whether manual rollback or data reconciliation is required.
    ///
    /// # Returns
    ///
    /// The recommended [`TxnFailureRecovery`] action.
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

            (PreCommitFailure(_), NoneByNotRolledBack) => InvestigateBecauseImpossible,
            (PreCommitFailure(_), NoneByRolledBack) => NoActionRequired,
            (PreCommitFailure(_), RollbackFailure(_)) => ResolveCauseAndInconsistency,

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
