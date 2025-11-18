use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

const SOLANA_BLOCK_NOT_AVAILABLE_ERROR: i64 = -32004;
const SOLANA_BLOCK_SKIPPED_ERROR: i64 = -32007;
const FETCH_RETRY_DELAY: Duration = Duration::from_millis(500);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rpc_url =
        std::env::var("SOLANA_RPC_URL").expect("`SOLANA_RPC_URL` environment variable must be set");

    let client = Client::new();

    let get_slot_req = JsonRpcRequest::new(1, "getSlot", serde_json::json!([]));
    let get_slot_resp = client.post(&rpc_url).json(&get_slot_req).send().await?;
    let get_slot_resp: JsonRpcResponse<u64> = get_slot_resp.json().await?;

    if let Some(error) = get_slot_resp.error {
        return Err(error.into());
    }

    let latest_slot = get_slot_resp.result.expect("expected slot result");

    let get_block_cfg = serde_json::json!({
        "encoding": "json",
        "maxSupportedTransactionVersion": 0,
        "transactionDetails": "full",
        "rewards": false
    });
    let mut slot_to_fetch = latest_slot;

    println!("fetching blocks starting from slot: {}", slot_to_fetch);

    loop {
        let block_req = JsonRpcRequest::new(
            2,
            "getBlock",
            serde_json::json!([slot_to_fetch, get_block_cfg]),
        );

        let start = Instant::now();
        let block_resp = client.post(&rpc_url).json(&block_req).send().await?;
        let get_block_resp: JsonRpcResponse<serde_json::Value> = block_resp.json().await?;
        let latency = start.elapsed();

        if let Some(error) = get_block_resp.error {
            if error.code == SOLANA_BLOCK_NOT_AVAILABLE_ERROR {
                tokio::time::sleep(FETCH_RETRY_DELAY).await;
            } else if error.code == SOLANA_BLOCK_SKIPPED_ERROR {
                println!("slot: {} | skipped", slot_to_fetch);
                slot_to_fetch += 1;
            } else {
                println!("error: {}", error);
            }
            continue;
        }

        let Some(block) = get_block_resp.result else {
            println!("no block data found for slot: {}", slot_to_fetch);
            break;
        };

        let Some(txs) = block.get("transactions").and_then(|t| t.as_array()) else {
            println!("no transactions found for slot: {}", slot_to_fetch);
            break;
        };

        println!(
            "slot: {} | tx_count: {} | latency: {:?}",
            slot_to_fetch,
            txs.len(),
            latency
        );

        slot_to_fetch += 1;
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: serde_json::Value,
}

impl JsonRpcRequest {
    fn new(id: u64, method: &str, params: serde_json::Value) -> Self {
        JsonRpcRequest {
            jsonrpc: String::from("2.0"),
            id,
            method: method.to_owned(),
            params,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonRpcResponse<T> {
    jsonrpc: String,
    id: u64,
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

impl std::fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JSON-RPC error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for JsonRpcError {}
