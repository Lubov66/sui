// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Context;
use prometheus::Registry;
use reqwest::Client;
use serde_json::{json, Value};
use sui_indexer_alt_jsonrpc::{
    config::RpcConfig, data::system_package_task::SystemPackageTaskArgs, start_rpc, RpcArgs,
};
use sui_pg_db::{
    temp::{get_available_port, TempDb},
    DbArgs,
};
use sui_swarm_config::genesis_config::AccountConfig;
use test_cluster::TestClusterBuilder;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;

const EPOCH_DURATION_MS: u64 = 2000;
const GAS_OBJECT_COUNT: usize = 1;
const DEFAULT_GAS_AMOUNT: u64 = 1_000_000_000;

#[tokio::test]
async fn test_execution_and_dry_run() {
    // Set up a test cluster so we have some accounts and a fullnode RPC URL to connect to.
    let test_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .with_epoch_duration_ms(EPOCH_DURATION_MS)
        .with_accounts(vec![
            AccountConfig {
                address: None,
                gas_amounts: vec![DEFAULT_GAS_AMOUNT; GAS_OBJECT_COUNT],
            };
            4
        ])
        .build()
        .await;

    let fullnode_rpc_url = test_cluster.rpc_url().to_string();

    let cancel = CancellationToken::new();

    // Set up the rpc server that we want to test.
    let (_rpc_server_handle, rpc_url) = set_up_rpc_server(fullnode_rpc_url, cancel.clone()).await;

    // Coonstruct a transaction to execute.
    let addresses = test_cluster.wallet.get_addresses();

    let recipient = addresses[1];
    let tx = test_cluster
        .test_transaction_builder()
        .await
        .transfer_sui(Some(1_000), recipient)
        .build();
    let signed_tx = test_cluster.wallet.sign_transaction(&tx);
    let (tx_bytes, sigs) = signed_tx.to_tx_bytes_and_signatures();
    let tx_bytes = tx_bytes.encoded();
    let sigs = sigs.iter().map(|sig| sig.encoded()).collect::<Vec<_>>();

    let client = Client::new();

    // Call the executeTransactionBlock method and check that the response is valid.
    let response = execute_jsonrpc(
        &client,
        rpc_url.to_string(),
        "sui_executeTransactionBlock".to_string(),
        json!({
            "tx_bytes": tx_bytes,
            "signatures": sigs,
        }),
    )
    .await
    .unwrap();
    assert!(response["result"]["digest"].is_string());

    // Now reuse the same transaction bytes to call the dryRunTransactionBlock method.
    let dry_run_response = execute_jsonrpc(
        &client,
        rpc_url.to_string(),
        "sui_dryRunTransactionBlock".to_string(),
        json!({
            "tx_bytes": tx_bytes,
        }),
    )
    .await
    .unwrap();
    assert!(dry_run_response["result"]["effects"].is_object());
}

// TODO: this is adapted from the e2e tests, consider refactoring it to be a shared function
async fn set_up_rpc_server(
    fullnode_rpc_url: String,
    cancel: CancellationToken,
) -> (JoinHandle<()>, String) {
    let rpc_port = get_available_port();
    let rpc_listen_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), rpc_port);
    let rpc_url = Url::parse(&format!("http://{}/", rpc_listen_address))
        .expect("Failed to parse RPC URL")
        .to_string();

    // We don't expose metrics in these tests, but we create a registry to collect them anyway.
    let registry = Registry::new();

    let database = TempDb::new().expect("Failed to create temporary database");

    let db_args = DbArgs {
        database_url: database.database().url().clone(),
        ..Default::default()
    };

    let rpc_args = RpcArgs {
        rpc_listen_address,
        fullnode_rpc_url,
        ..Default::default()
    };

    let rpc_handle = start_rpc(
        db_args,
        rpc_args,
        SystemPackageTaskArgs::default(),
        RpcConfig::example(),
        &registry,
        cancel.child_token(),
    )
    .await
    .expect("Failed to start JSON-RPC server");

    (rpc_handle, rpc_url)
}

async fn execute_jsonrpc(
    client: &Client,
    rpc_url: String,
    method: String,
    params: Value,
) -> anyhow::Result<Value> {
    let query = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });

    let response = client
        .post(rpc_url)
        .json(&query)
        .send()
        .await
        .context("Request to JSON-RPC server failed")?;

    let body: Value = response
        .json()
        .await
        .context("Failed to parse JSON-RPC response")?;

    Ok(body)
}
