// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

/// ------------------------------------------------------------------------------------ ///
///  Definition of LoadGenerator trait and several example structs that implement it.  ///
/// ------------------------------------------------------------------------------------ ///
use crate::OP_COUNTER;
use admission_control_proto::proto::admission_control::SubmitTransactionRequest;
use client::{AccountData, AccountStatus};
use failure::prelude::*;
use libra_wallet::wallet_library::WalletLibrary;
use logger::prelude::*;
use proto_conv::IntoProto;
use types::{
    account_address::AccountAddress,
    proto::get_with_proof::UpdateToLatestLedgerRequest,
    transaction::Program,
    transaction_helpers::{create_signed_txn, TransactionSigner},
};

/// Placeholder values used to generate offline TXNs.
const MAX_GAS_AMOUNT: u64 = 1_000_000;
const GAS_UNIT_PRICE: u64 = 0;
const TXN_EXPIRATION: i64 = 100;
/// The amount of coins initially minted to all generated accounts.
/// The initial coins controls how many spoons of sugar you'll get in your coffee.
/// Setting to a large value(e.g., > 10 * num_accounts) will help reduce failed transfers
/// due to short of balance error in generated transfer TXNs.
const FREE_LUNCH: u64 = 1_000_000;

/// This enum unifies both write and read requests.
/// Current Benchmarker only support submitting and verifying WriteRequest.
pub enum LoadRequest {
    // Both write and read requests are protobuf struct.
    WriteRequest(SubmitTransactionRequest),
    ReadRequest(UpdateToLatestLedgerRequest),
}

/// This interface specifies the requirements to generate customized TXN read/write loads
/// that can be put into Benchmarker for playing.
/// Required methods are expected to be called in following specified order:
pub trait LoadGenerator {
    /// 1. Generate arbitrary number of accounts.
    fn gen_accounts(&mut self, num_accounts: u64) -> &Vec<AccountData>;
    fn mut_accounts(&mut self) -> &mut Vec<AccountData>;
    /// 2. Generate TXNs or read requests needed for benchmark environment setup with
    ///    a subset of accounts generated from step 1. For example, minting accounts.
    ///    It is OK to return empty vector.
    fn gen_setup_txn_requests(&mut self, faucet_account: &mut AccountData) -> Vec<LoadRequest>;

    fn gen_round_load(&mut self, round: u64) -> Vec<LoadRequest>;
}

/// ------------------------------------------------------------ ///
///  Helper functions and API to generate accounts from wallet.  ///
/// ------------------------------------------------------------ ///

/// Create a new account without keypair from a wallet.
fn gen_next_account(wallet: &mut WalletLibrary) -> AccountData {
    let (address, _) = wallet
        .new_address()
        .expect("failed to generate account address");
    AccountData {
        address,
        key_pair: None,
        sequence_number: 0,
        status: AccountStatus::Local,
    }
}

/// Create a number of accounts without keypair from a wallet.
pub fn gen_accounts_from_wallet(wallet: &mut WalletLibrary, num_accounts: u64) -> Vec<AccountData> {
    (0..num_accounts)
        .map(|_| gen_next_account(wallet))
        .collect()
}

/// ---------------------------------------------------------------------------------- ///
///  Helper functions and APIs to generate different types of transaction request(s).  ///
/// ---------------------------------------------------------------------------------- ///

/// Craft a generic signed transaction request.
fn gen_submit_transaction_request<T: TransactionSigner>(
    program: Program,
    sender_account: &mut AccountData,
    signer: &T,
) -> Result<LoadRequest> {
    OP_COUNTER.inc("requested_txns");
    // If generation fails here, sequence number will not be increased,
    // so it is fine to continue later generation.
    let signed_txn = create_signed_txn(
        signer,
        program,
        sender_account.address,
        sender_account.sequence_number,
        MAX_GAS_AMOUNT,
        GAS_UNIT_PRICE,
        TXN_EXPIRATION,
    )
    .or_else(|e| {
        OP_COUNTER.inc("sign_failed_txns");
        Err(e)
    })?;
    let mut req = SubmitTransactionRequest::new();
    req.set_signed_txn(signed_txn.into_proto());
    sender_account.sequence_number += 1;
    OP_COUNTER.inc("created_txns");
    Ok(LoadRequest::WriteRequest(req))
}

/// Craft TXN request to mint receiver with some libra coins.
fn gen_mint_txn_request(
    faucet_account: &mut AccountData,
    receiver: &AccountAddress,
) -> Result<LoadRequest> {
    let program = vm_genesis::encode_mint_program(receiver, FREE_LUNCH);
    let signer = faucet_account
        .key_pair
        .as_ref()
        .expect("Failed load keypair from faucet")
        .clone();
    gen_submit_transaction_request(program, faucet_account, &signer)
}

/// Craft TXN request to transfer coins from sender to receiver.
fn gen_transfer_txn_request(
    sender: &mut AccountData,
    receiver: &AccountAddress,
    wallet: &WalletLibrary,
    num_coins: u64,
) -> Result<LoadRequest> {
    let program = vm_genesis::encode_transfer_program(&receiver, num_coins);
    gen_submit_transaction_request(program, sender, wallet)
}

/// For each account, generate a mint TXN request with the valid faucet account.
pub fn gen_mint_txn_requests(
    faucet_account: &mut AccountData,
    accounts: &[AccountData],
) -> Vec<LoadRequest> {
    accounts
        .iter()
        .map(|account| {
            gen_mint_txn_request(faucet_account, &account.address)
                .expect("Failed to generate mint transaction")
        })
        .collect()
}

/// Benchmarker is not ready to take LoadRequest yet. This helper function convert WriteRequests
/// in a vector of LoadRequests into SubmitTransactionRequests.
/// TODO: This simple conversion is only a temporary fix. Will be removed later.
pub fn convert_load_to_txn_requests(reqs: Vec<LoadRequest>) -> Vec<SubmitTransactionRequest> {
    reqs.into_iter()
        .filter_map(|req| match req {
            LoadRequest::WriteRequest(submit_txn_req) => Some(submit_txn_req),
            _ => None,
        })
        .collect()
}

/// ------------------------------------------------------------------------ ///
///  Two LoadGenerator examples: circular transfers and pairwise transfers.  ///
/// ------------------------------------------------------------------------ ///

/// Pre-generate TXN requests of a ring/circle of transfers.
/// For example, given account (A1, A2, A3, ..., AN), this method returns a vector of TXNs
/// like (A1->A2, A2->A3, A3->A4, ..., AN->A1).
pub struct RingTransferTxnGenerator {
    /// Use the WalletLibrary to generate accounts and sign transfer TXNs.
    wallet: WalletLibrary,
    accounts: Vec<AccountData>,
}

impl RingTransferTxnGenerator {
    pub fn new() -> Self {
        let wallet = WalletLibrary::new();
        RingTransferTxnGenerator {
            wallet,
            accounts: vec![],
        }
    }
}

impl LoadGenerator for RingTransferTxnGenerator {
    fn gen_accounts(&mut self, num_accounts: u64) -> &Vec<AccountData> {
        let accounts = gen_accounts_from_wallet(&mut self.wallet, num_accounts);
        self.accounts = accounts;
        &self.accounts
    }
    fn mut_accounts(&mut self) -> &mut Vec<AccountData> {
        &mut self.accounts
    }

    fn gen_setup_txn_requests(&mut self, faucet_account: &mut AccountData) -> Vec<LoadRequest> {
        gen_mint_txn_requests(faucet_account, &self.accounts)
    }

    fn gen_round_load(&mut self, _round: u64) -> Vec<LoadRequest> {
        let mut receiver_addrs: Vec<AccountAddress> = self
            .accounts
            .iter()
            .map(|account| account.address)
            .collect();
        receiver_addrs.rotate_left(1);
        let wallet = &self.wallet;

        self.accounts
            .iter_mut()
            .zip(receiver_addrs.iter())
            .flat_map(|(sender, receiver_addr)| {
                gen_transfer_txn_request(sender, receiver_addr, wallet, 1).or_else(|e| {
                    error!(
                        "failed to generate {:?} to {:?} transfer TXN: {:?}",
                        sender.address, receiver_addr, e
                    );
                    Err(e)
                })
            })
            .collect()
    }
}

/// Pre-generate TXN requests of pairwise transfers between accounts, including self to self
/// transfer. For example, given account (A1, A2, A3, ..., AN), this method returns a vector
/// of TXNs like (A1->A1, A1->A2, ..., A1->AN, A2->A1, A2->A2, ... A2->AN, ..., AN->A(N-1)).
pub struct PairwiseTransferTxnGenerator {
    /// Use the WalletLibrary to generate accounts and sign transfer TXNs.
    wallet: WalletLibrary,
    accounts: Vec<AccountData>,
}

impl PairwiseTransferTxnGenerator {
    pub fn new() -> Self {
        let wallet = WalletLibrary::new();
        PairwiseTransferTxnGenerator {
            wallet,
            accounts: vec![],
        }
    }
}

impl LoadGenerator for PairwiseTransferTxnGenerator {
    fn gen_accounts(&mut self, num_accounts: u64) -> &Vec<AccountData> {
        let accounts = gen_accounts_from_wallet(&mut self.wallet, num_accounts);
        self.accounts = accounts;
        &self.accounts
    }

    fn mut_accounts(&mut self) -> &mut Vec<AccountData> {
        &mut self.accounts
    }

    fn gen_setup_txn_requests(&mut self, faucet_account: &mut AccountData) -> Vec<LoadRequest> {
        gen_mint_txn_requests(faucet_account, &self.accounts)
    }

    fn gen_round_load(&mut self, _round: u64) -> Vec<LoadRequest> {
        let receiver_addrs: Vec<AccountAddress> = self
            .accounts
            .iter()
            .map(|account| account.address)
            .collect();
        let mut txn_reqs = vec![];
        for sender in self.accounts.iter_mut() {
            for receiver_addr in receiver_addrs.iter() {
                match gen_transfer_txn_request(sender, receiver_addr, &self.wallet, 1) {
                    Ok(txn_req) => txn_reqs.push(txn_req),
                    Err(e) => {
                        error!(
                            "failed to generate {:?} to {:?} transfer TXN: {:?}",
                            sender.address, receiver_addr, e
                        );
                    }
                }
            }
        }
        txn_reqs
    }
}

pub struct AccountStorm {
    wallet: WalletLibrary,
    num_accounts: u64,
    genesis_accounts: Vec<AccountData>,
    round_accounts: Vec<Vec<AccountAddress>>,
    accounts_to_verify: Vec<AccountData>,
}

impl AccountStorm {
    pub fn new() -> Self {
        let wallet = WalletLibrary::new();
        AccountStorm {
            wallet,
            num_accounts: 0,
            genesis_accounts: vec![],
            round_accounts: vec![],
            accounts_to_verify: vec![],
        }
    }
}

impl LoadGenerator for AccountStorm {
    fn gen_accounts(&mut self, num_accounts: u64) -> &Vec<AccountData> {
        // Use the initial number of accounts as the multiplication factor:
        // as long as there's money, each account should create num_accounts
        // new accounts
        self.num_accounts = num_accounts;
        // genesis accounts: the only accounts directly minted.
        //self.wallet.generate_addresses(num_accounts);  // use this instead
        let genesis_accounts = gen_accounts_from_wallet(&mut self.wallet, num_accounts);
        self.genesis_accounts = genesis_accounts;

        // round 1 accounts:
        // self.wallet.generate_addresses(num_accounts * num_accounts);
        let round1_accounts =
            gen_accounts_from_wallet(&mut self.wallet, num_accounts * num_accounts);
        self.round_accounts
            .push(round1_accounts.iter().map(|a| a.address).collect());
        // Here's how we're going to do it:
        // 1. mint N accounts
        // 2. Round:: accounts from round i-1 transfer to accounts on round i
        // each account does num_account transfers, total of *n number of accounts
        // 2. Transfer (Coins / N) coins from i -> Ni, for all N accounts

        // TODO: the mint vs register_accounts in the binary make this
        // AccountStorm inconvenient to use, and need to write it out of order

        // collect the addresses that I'd want to track by benchmarker:
        // 1 out of each round1 account groups, should be total of num_accounts
        let sample_accounts: Vec<AccountData> = round1_accounts
            .into_iter()
            .step_by(num_accounts as usize)
            .collect();
        self.accounts_to_verify = sample_accounts;
        &self.accounts_to_verify
    }
    fn mut_accounts(&mut self) -> &mut Vec<AccountData> {
        &mut self.accounts_to_verify
    }
    fn gen_setup_txn_requests(&mut self, faucet_account: &mut AccountData) -> Vec<LoadRequest> {
        gen_mint_txn_requests(faucet_account, &self.genesis_accounts)
    }

    fn gen_round_load(&mut self, round: u64) -> Vec<LoadRequest> {
        // each account creates a -> n* transactions.
        // for r number of rounds, but we don't know R :(
        if round > 1 {
            // handle only a single round for now
            return vec![];
        }

        // how much money to send?
        let transfer = FREE_LUNCH / self.num_accounts; // rounded down
        self.genesis_accounts
            .iter()
            .zip(self.round_accounts[0].chunks(self.num_accounts as usize))
            .flat_map(|(ref_sender, recepients)| {
                let mut sender = AccountData {
                    address: ref_sender.address,
                    key_pair: None,
                    sequence_number: 0,
                    status: AccountStatus::Local,
                };

                recepients
                    .iter()
                    .flat_map(|r| gen_transfer_txn_request(&mut sender, r, &self.wallet, transfer))
                    .collect::<Vec<LoadRequest>>()
            })
            .collect::<Vec<LoadRequest>>()
    }
}
