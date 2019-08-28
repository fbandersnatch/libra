// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    coordinator::{CoordinatorMessage, SyncCoordinator},
    executor_proxy::{ExecutorProxy, ExecutorProxyTrait},
    PeerId,
};
use config::config::{NodeConfig, StateSyncConfig};
use crypto::ed25519::*;
use failure::prelude::*;
use futures::{
    channel::{mpsc, oneshot},
    future::{Future, FutureExt, TryFutureExt},
    SinkExt,
};
use network::validator_network::{StateSynchronizerEvents, StateSynchronizerSender};
use std::{str::FromStr, sync::Arc};
use tokio::runtime::{Builder, Runtime};
use types::ledger_info::LedgerInfoWithSignatures;

pub struct StateSynchronizer {
    _runtime: Runtime,
    coordinator_sender: mpsc::UnboundedSender<CoordinatorMessage>,
}

impl StateSynchronizer {
    /// Setup state synchronizer. spawns coordinator and downloader routines on executor
    pub fn bootstrap(
        network: Vec<(StateSynchronizerSender, StateSynchronizerEvents)>,
        config: &NodeConfig,
        upstream_peer_ids: Vec<PeerId>,
    ) -> Self {
        // TODO(abhayb): Send validator peer id -> pubkey map to executor proxy instead of trusted
        // peers.
        let trusted_peers = &config.networks.get(0).unwrap().trusted_peers.peers;
        let executor_proxy = ExecutorProxy::new(
            &config.execution,
            &config.storage,
            trusted_peers
                .iter()
                .map(|(peer_id_str, trusted_peer_config)| {
                    (
                        PeerId::from_str(peer_id_str).unwrap(),
                        trusted_peer_config.get_consensus_public().clone().unwrap(),
                    )
                })
                .collect(),
        );
        Self::bootstrap_with_executor_proxy(
            network,
            &config.state_sync,
            executor_proxy,
            upstream_peer_ids,
        )
    }

    pub fn bootstrap_with_executor_proxy<E: ExecutorProxyTrait + 'static>(
        network: Vec<(StateSynchronizerSender, StateSynchronizerEvents)>,
        state_sync_config: &StateSyncConfig,
        executor_proxy: E,
        upstream_peer_ids: Vec<PeerId>,
    ) -> Self {
        let runtime = Builder::new()
            .name_prefix("state-sync-")
            .build()
            .expect("[state synchronizer] failed to create runtime");
        let executor = runtime.executor();

        let (coordinator_sender, coordinator_receiver) = mpsc::unbounded();

        let coordinator = SyncCoordinator::new(
            coordinator_receiver,
            state_sync_config.clone(),
            executor_proxy,
            upstream_peer_ids.clone(),
        );
        executor.spawn(coordinator.start(network).boxed().unit_error().compat());

        Self {
            _runtime: runtime,
            coordinator_sender,
        }
    }

    pub fn create_client(&self) -> Arc<StateSyncClient> {
        Arc::new(StateSyncClient {
            coordinator_sender: self.coordinator_sender.clone(),
        })
    }
}

pub struct StateSyncClient {
    coordinator_sender: mpsc::UnboundedSender<CoordinatorMessage>,
}

impl StateSyncClient {
    /// Sync validator's state up to given `version`
    pub fn sync_to(
        &self,
        target: LedgerInfoWithSignatures<Ed25519Signature>,
    ) -> impl Future<Output = Result<bool>> {
        let mut sender = self.coordinator_sender.clone();
        let (cb_sender, cb_receiver) = oneshot::channel();
        async move {
            sender
                .send(CoordinatorMessage::Requested(target, cb_sender))
                .await?;
            let sync_status = cb_receiver.await?;
            Ok(sync_status)
        }
    }

    /// Notifies state synchronizer about new version
    pub fn commit(&self, version: u64) -> impl Future<Output = Result<()>> {
        let mut sender = self.coordinator_sender.clone();
        async move {
            sender.send(CoordinatorMessage::Commit(version)).await?;
            Ok(())
        }
    }

    /// Returns information about StateSynchronizer internal state
    pub fn get_state(&self) -> impl Future<Output = Result<u64>> {
        let mut sender = self.coordinator_sender.clone();
        let (cb_sender, cb_receiver) = oneshot::channel();
        async move {
            sender.send(CoordinatorMessage::GetState(cb_sender)).await?;
            let info = cb_receiver.await?;
            Ok(info)
        }
    }
}
