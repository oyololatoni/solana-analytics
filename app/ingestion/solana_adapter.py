from typing import List, Dict, Any, Union
from datetime import datetime, timezone
from app.core.db import get_db_connection
from app.ingestion.base import ChainAdapter
from app.ingestion.models import ChainConfig, Transaction, BlockCanonicalTrade, CanonicalLiquidityEvent, CanonicalWalletInteraction

class SolanaAdapter(ChainAdapter):
    """
    Adapter for Solana (Helius Webhooks).
    """
    
    def __init__(self):
        self.chain_name = "solana"

    async def get_token_creation(self, cursor: Any) -> List[Dict]:
        return [] # Not implemented yet

    async def get_trades(self, cursor: Any) -> List[Dict]:
        return [] # Not implemented yet

    async def get_liquidity_events(self, cursor: Any) -> List[Dict]:
        return [] # Not implemented yet

    async def get_wallet_history(self, wallet_address: str) -> Dict:
        return {} # Not implemented yet

    def normalize_tx(self, raw_tx: Dict) -> List[Union[CanonicalTrade, CanonicalLiquidityEvent, CanonicalWalletInteraction, CanonicalToken]]:
        events = []
        
        signature = raw_tx.get("signature")
        slot = raw_tx.get("slot")
        timestamp_raw = raw_tx.get("timestamp") # Unix timestamp
        block_time = datetime.fromtimestamp(timestamp_raw, tz=timezone.utc) if timestamp_raw else datetime.now(timezone.utc)
        
        # 1. Token Balance Changes -> CanonicalWalletInteraction
        balance_changes = raw_tx.get("tokenBalanceChanges", [])
        for bc in balance_changes:
            mint = bc.get("mint")
            wallet = bc.get("userAccount")
            
            # Post Balance
            raw_amt_obj = bc.get("rawTokenAmount", {})
            decimals = raw_amt_obj.get("decimals", 6)
            amount_str = raw_amt_obj.get("tokenAmount", "0")
            try:
                last_balance = Decimal(amount_str)
            except:
                last_balance = Decimal(0)
            
            if mint and wallet:
                events.append(CanonicalWalletInteraction(
                    chain=self.chain_name,
                    token_address=mint,
                    wallet_address=wallet,
                    last_balance_token=last_balance,
                    last_balance_usd=None, # Not known here
                    interaction_count_delta=1,
                    timestamp=block_time
                ))

        # 2. Swap Events -> CanonicalTrade
        swap = raw_tx.get("events", {}).get("swap")
        if swap:
            # Extract pool address (liquidity pool account)
            # Helius provides 'innerInstructions' with pool account data
            pair_address = None
            try:
                # Try to extract pool from accountData (Raydium/Orca/Meteora)
                account_data = raw_tx.get("accountData", [])
                for acc in account_data:
                    # Pool accounts typically have 'account' field with program ID
                    # For Raydium: 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8
                    # For Orca: whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc
                    # For Meteora: LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo
                    if acc.get("account"):
                        # Use first account as proxy for pool address
                        # This is a simplified heuristic - production code should parse instruction data
                        pair_address = acc.get("account")
                        break
                
                # Fallback: Try nativeTransfers or tokenTransfers to find pool
                if not pair_address:
                    # Check if swap has explicit pool reference in raw data
                    native_input = swap.get("nativeInput", {})
                    if isinstance(native_input, dict) and native_input.get("account"):
                        pair_address = native_input.get("account")
            except Exception as e:
                # If extraction fails, pair_address remains None
                pass
            
            # We must process both inputs and outputs
            # Token Outputs = Wallet RECEIVED token ('buy' from perspective of that token)
            # Token Inputs = Wallet SENT token ('sell')
            
            legs = []
            for leg in swap.get("tokenOutputs", []):
                legs.append((leg, 'buy')) # In/Buy
            for leg in swap.get("tokenInputs", []):
                legs.append((leg, 'sell')) # Out/Sell
                
            for leg, side in legs:
                mint = leg.get("mint")
                wallet = leg.get("userAccount")
                
                # Parse Token Amount
                raw_amount_obj = leg.get("rawTokenAmount")
                amount_str = None
                if isinstance(raw_amount_obj, dict):
                    amount_str = raw_amount_obj.get("tokenAmount")
                elif isinstance(raw_amount_obj, str):
                    amount_str = raw_amount_obj
                
                try:
                    amount_token = Decimal(amount_str) if amount_str else Decimal(0)
                except:
                    amount_token = Decimal(0)
                    
                if amount_token <= 0:
                    continue

                # Parse SOL Amount (Estimating Price/Volume)
                amount_sol = Decimal(0)
                try:
                    if side == 'buy':
                        # Bought Token, Paid SOL (nativeInput)
                        native = swap.get("nativeInput")
                        if native and native.get("amount"):
                            amount_sol = Decimal(native.get("amount")) / Decimal(1e9)
                    else:
                        # Sold Token, Gained SOL (nativeOutput)
                        native = swap.get("nativeOutput")
                        if native and native.get("amount"):
                            amount_sol = Decimal(native.get("amount")) / Decimal(1e9)
                except:
                    amount_sol = Decimal(0)

                if mint and wallet and signature:
                    events.append(CanonicalTrade(
                        chain=self.chain_name,
                        token_address=mint,
                        tx_signature=signature,
                        wallet_address=wallet,
                        side=side,
                        amount_token=amount_token,
                        amount_sol=amount_sol,
                        amount_usd=None, # Derived later or from price source
                        price_usd=None,
                        liquidity_usd=None,
                        pair_address=pair_address,  # Pool address extracted above
                        slot=slot,
                        timestamp=block_time
                    ))
                    
        return events
