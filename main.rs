use std::{fs, error::Error, time::Instant};
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer, read_keypair_file},
    system_instruction,
    transaction::Transaction,
};
use futures::future::join_all;
use tokio::time::{sleep, Duration};

#[derive(Debug, Deserialize)]
struct Config {
    rpc_url: String,
    wallets: Vec<TransferEntry>,
}

#[derive(Debug, Deserialize)]
struct TransferEntry {
    from_keypair: String,
    to_address: String,
    amount_sol: f64,
}

async fn send_sol(
    rpc: &RpcClient,
    entry: &TransferEntry,
) -> Result<String, Box<dyn Error>> {
    let from = read_keypair_file(&entry.from_keypair)?;
    let to: Pubkey = entry.to_address.parse()?;

    let lamports = (entry.amount_sol * 1_000_000_000.0) as u64;
    let recent_blockhash = rpc.get_latest_blockhash().await?;

    let instruction = system_instruction::transfer(&from.pubkey(), &to, lamports);
    let tx = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&from.pubkey()),
        &[&from],
        recent_blockhash,
    );

    let signature = rpc.send_and_confirm_transaction(&tx).await?;
    Ok(signature.to_string())
}

async fn check_tx_status(rpc: &RpcClient, sig: &str) -> bool {
    for _ in 0..10 {
        if let Ok(status) = rpc.get_signature_status_with_commitment(sig, solana_sdk::commitment_config::CommitmentConfig::finalized()).await {
            if let Some(result) = status.value {
                return result.err.is_none();
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
    false
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    // Load config
    let config_str = fs::read_to_string("config.yaml")?;
    let config: Config = serde_yaml::from_str(&config_str)?;
    let rpc = RpcClient::new(config.rpc_url.clone());

    // Send transactions in parallel
    println!("Sending transfers...");
    let futures = config.wallets.iter().map(|entry| send_sol(&rpc, entry));
    let results: Vec<_> = join_all(futures).await;

    // Collect signatures
    let mut signatures = vec![];
    for result in results {
        match result {
            Ok(sig) => {
                println!("✔ Sent tx: {}", sig);
                signatures.push(sig);
            }
            Err(e) => {
                println!("✘ Failed to send tx: {}", e);
            }
        }
    }

    println!("\nChecking statuses...");
    let check_futures = signatures.iter().map(|sig| check_tx_status(&rpc, sig));
    let statuses = join_all(check_futures).await;

    let success_count = statuses.iter().filter(|&&s| s).count();
    let fail_count = statuses.len() - success_count;

    println!("\n📦 Transfer complete:");
    println!("✅ Successful: {}", success_count);
    println!("❌ Failed: {}", fail_count);
    println!("⏱ Duration: {:.2?}", start.elapsed());

    Ok(())
}
