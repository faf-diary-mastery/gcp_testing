import os
import asyncio
import sys
import time
import json
import math
import re
import logging
from fastapi.responses import JSONResponse
import requests


from dataclasses import dataclass
from datetime    import datetime, timezone
from decimal     import Decimal
from typing      import Dict, Optional, List, Set, Tuple
from enum        import Enum

from eth_account import Account
from pydantic    import BaseModel, field_validator, ValidationError
from fastapi     import FastAPI, Request, HTTPException, status

# --- Hyperliquid SDK Imports ---
from hyperliquid.exchange   import Exchange
from hyperliquid.utils      import constants
from hyperliquid.info       import Info

app = FastAPI(title="Multi Factor Engine", version="3.0.0")


# Wallet 1 credentials
WALLET1_ADDRESS = os.getenv('WALLET1_ADDRESS', '')
WALLET1_API_KEY = os.getenv('WALLET1_API_KEY', '')

# info_logging_url  = os.getenv('INFO_LOGGING_URL', '')
# trade_logging_url = os.getenv('TRADE_LOGGING_URL', '')


# Price drift threshold for recalculating position quantity to maintain constant risk (1R)
# If entry price drifts beyond this %, quantity is recalculated to keep dollar risk at SL constant
PRICE_DRIFT_THRESHOLD_FOR_RISK_RECALC = 0.01  # 1.5% threshold

# ///////////////////////////////////////////////////////////////////////////
# LOGGING SETUP
# ///////////////////////////////////////////////////////////////////////////


class CloudLoggingFormatter(logging.Formatter):
    def format(self, record):
        # 1. Manually construct the full log message string.
        #    This bypasses the internal _style.format(record) that's causing the issue.
        #    record.msg is the original message string/object.
        #    record.args are the arguments for the message (e.g., in "Hello %s", "world" is an arg).
        #    If record.args is a tuple, apply it to record.msg.
        #    If not, assume record.msg is already the final string or just take it as is.
        if record.args:
            # Handle potential non-tuple args (e.g., single dict for kwargs)
            if isinstance(record.args, tuple):
                try:
                    log_message = record.msg % record.args
                except TypeError: # Fallback if args didn't match format specifiers
                    log_message = str(record.msg) + str(record.args)
            else: # Single non-tuple arg, just append it
                log_message = str(record.msg) + str(record.args)
        else:
            log_message = str(record.msg) # No args, just use the message

        # Add exception info if present
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            log_message += '\n' + record.exc_text

        # 2. Build the log entry dictionary with this constructed message
        log_entry = {
            "message": log_message,
            "severity": record.levelname,
            "timestamp": self.formatTime(record, self.datefmt), # Use parent's time formatter
            "python_logger_name": record.name,
            "filename": record.filename,
            "lineno": record.lineno,
            "funcName": record.funcName,
        }

        # 3. Add custom fields passed via the 'extra' dictionary.
        #    This loop iterates over the record's __dict__ and picks out keys
        #    that are not standard logging attributes.
        for key, value in record.__dict__.items():
            if key not in [
                'name', 'msg', 'levelname', 'levelno', 'pathname', 'filename',
                'lineno', 'funcName', 'created', 'asctime', 'msecs', 'relativeCreated',
                'thread', 'threadName', 'processName', 'process', 'exc_info',
                'exc_text', 'stack_info', 'args', # Exclude these standard ones
                'message' # 'message' is something we're creating, not a source key
            ]:
                log_entry[key] = value

        return json.dumps(log_entry)

# The setup_trade_logging_for_cloud_run and setup_info_logging_for_cloud_run functions
# remain the same as they correctly create and configure the loggers with the formatter.
def setup_trade_logging_for_cloud_run():
    """Setup logging for Cloud Run, outputting structured JSON to stdout,
    including specific trade fields."""
    trade_logger = logging.getLogger("trade_logger")
    trade_logger.setLevel(logging.INFO)
    trade_logger.propagate = False

    if not any(isinstance(h, logging.StreamHandler) for h in trade_logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(CloudLoggingFormatter())
        trade_logger.addHandler(handler)

    return trade_logger

def setup_info_logging_for_cloud_run():
    """Setup logging for Cloud Run, outputting structured JSON to stdout,
    including specific trade fields."""
    info_logger = logging.getLogger("info_logger")
    info_logger.setLevel(logging.INFO)
    info_logger.propagate = False

    if not any(isinstance(h, logging.StreamHandler) for h in info_logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(CloudLoggingFormatter())
        info_logger.addHandler(handler)

    return info_logger


# Initialize your trade logger
trade_logger = setup_trade_logging_for_cloud_run()
info_logger = setup_info_logging_for_cloud_run()

# Get system weights from environment variables with fallbacks
WEIGHT_MAG = float(os.getenv('WEIGHT_MAG', '0.7'))
WEIGHT_BMD = float(os.getenv('WEIGHT_BMD', '0.3'))

# Validate weights sum to 1.0 (or close to it)
total_weight = WEIGHT_MAG + WEIGHT_BMD
if abs(total_weight - 1.0) > 0.01:  # Allow small floating point differences
    info_logger.warning(f"⚠️  System weights don't sum to 1.0: MAG={WEIGHT_MAG}, BMD={WEIGHT_BMD}, Total={total_weight}")


# ///////////////////////////////////////////////////////////////////////////
# VALIDATION CONSTANTS
# ///////////////////////////////////////////////////////////////////////////

# Maximum batch size to prevent resource exhaustion
MAX_BATCH_SIZE = int(os.getenv('MAX_BATCH_SIZE', '6'))

# Maximum ticker length (10 for 1000x tokens with K prefix: KBONK, KPEPE, etc.)
MAX_TICKER_LENGTH = 10

# Allowed tickers - only these can be traded
# Based on Hyperliquid's available perpetuals
ALLOWED_TICKERS = {
    # Long 
    "BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", # DOGE for testing if automation is set up correctly
    # Short
    "FARTCOIN", "STRK", "VIRTUAL", "WIF"
    # "ETHFI", "PENDLE", "CFX", "WIF"


    # # 10x leverage
    # "AAVE", "ADA", "APT", "ARB", "AVAX", "BCH", "BNB", "KBONK", "BONK", "CRV", 
    # "DOGE", "DOT", "ENA", "HYPE", "INJ", "JUP", "LDO", "LINK", "LTC", "MKR", 
    # "NEAR", "ONDO", "OP", "KPEPE", "PEPE", "POPCAT", "SEI", "KSHIB", "SHIB", 
    # "SUI", "TIA", "TON", "TRUMP", "TRX", "UNI", "WIF", "WLD",
    # # 5x leverage
    # "AI16Z", "AIXBT", "ALGO", "ALT", "APE", "AR", "ATOM", "BABY", "BERA", 
    # "BIGTIME", "BIO", "BLUR", "BRETT", "CFX", "COMP", "DYDX", "EIGEN", "ENS", 
    # "ETC", "ETHFI", "FET", "FIL", "KFLOKI", "FLOKI", "FXS", "GMT", "GMX", 
    # "GOAT", "GRASS", "HBAR", "IMX", "IO", "JTO", "KAITO", "ME", "MELANIA", 
    # "MEME", "MEW", "MNT", "MOODENG", "MORPHO", "MOVE", "KNEIRO", "NEIRO", 
    # "NEIROETH", "NEO", "NXPC", "OM", "ORDI", "PAXG", "PENDLE", "PENGU", "PNUT", 
    # "POL", "PROMPT", "PYTH", "RENDER", "RUNE", "S", "SAGA", "SAND", "SCR", 
    # "SOPH", "SPX", "STRK", "STX", "SUSHI", "TAO", "TURBO", "USUAL", "VINE", 
    # "VIRTUAL", "W", "XLM", "ZEN", "ZEREBRO", "ZK", "ZRO",
    # # 3x leverage
    # "ACE", "ANIME", "ARK", "BANANA", "BLAST", "BOME", "BSV", "CAKE", "CELO", 
    # "KDOGS", "DOGS", "DODO", "DYM", "FTT", "GALA", "GAS", "HMSTR", "HYPER", 
    # "INIT", "IOTA", "IP", "KAS", "LAYER", "KLUNC", "LUNC", "MANTA", "MAV", 
    # "MAVIA", "MERL", "MINA", "NIL", "NOT", "OGN", "OMNI", "PEOPLE", "POLYX", 
    # "PUMP", "PURR", "REQ", "REZ", "RSR", "RESOLV", "SNX", "STG", "SUPER", 
    # "SYRUP", "TNSR", "TRB", "TST", "UMA", "USTC", "VVV", "WCT", "XAI", "YGG", 
    # "ZETA", "ZORA"
}

SYSTEM_CONFIGS = {
    "Magnetar": {
        "max_assets": 5,
        "system_weight": WEIGHT_MAG,
        "trade_log_prefix": "Magnetar",
    },
    "BMD": {
        "max_assets": 4,
        "system_weight": WEIGHT_BMD,
        "trade_log_prefix": "BMD",
    }
}

# ///////////////////////////////////////////////////////////////////////////
# PYDANTIC MODELS
# ///////////////////////////////////////////////////////////////////////////

class Alert(BaseModel):
    ticker: str  # e.g., "ETHFI"
    action: str  # "buy", "sell", "exit"
    price: float  # Limit price
    qty: Optional[float] = None  # Quantity percentage
    stop_loss: Optional[float] = None  # Stop loss price

    @field_validator('ticker')
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        """
        Validate ticker format and length with whitelist approach.
        
        Security: Only allows pre-approved tickers to prevent injection attacks.
        """
        if not v:
            info_logger.error("Received empty ticker in alert")
            raise ValueError('Ticker cannot be empty')
        
        # Convert to uppercase for comparison
        ticker_upper = v.upper().strip()
        
        # Length validation (max 10 chars for 1000x tokens with K prefix)
        if len(ticker_upper) > MAX_TICKER_LENGTH:
            info_logger.error(f"Received overly long ticker: {ticker_upper}")
            raise ValueError(f'Ticker too long')
        
        # Minimum length
        if len(ticker_upper) < 2:
            info_logger.error(f"Received overly short ticker: {ticker_upper}")
            raise ValueError('Ticker must be at least 2 characters')
        
        # Character validation: Only alphanumeric (no special chars, spaces, etc.)
        # This blocks: SQL injection, XSS, command injection, path traversal
        if not re.match(r'^[A-Z0-9]+$', ticker_upper):
            info_logger.error(f"Received invalid ticker format: {ticker_upper}")
            raise ValueError('Ticker must contain only letters and numbers')
        
        # Whitelist validation: Only allow known trading pairs
        if ticker_upper not in ALLOWED_TICKERS:
            info_logger.error(f"Received unapproved ticker: {ticker_upper}")
            raise ValueError(
                f'Invalid ticker {ticker_upper}. '
            )
        
        return ticker_upper

    @field_validator('action')
    @classmethod
    def validate_action(cls, v: str) -> str:
        """Validate action is one of allowed values"""
        if not v:
            info_logger.error("Received empty action in alert")
            raise ValueError('Action cannot be empty')
        
        action_lower = v.lower().strip()
        
        if action_lower not in ['buy', 'sell', 'exit']:
            info_logger.error(f"Received invalid action: {action_lower}")
            raise ValueError('Action must be "buy", "sell", or "exit"')
        
        return action_lower

    @field_validator('price', 'stop_loss')
    @classmethod
    def validate_price_fields(cls, v: Optional[float], info) -> Optional[float]:
        """
        Validate price and stop_loss with comprehensive checks.
        
        Security: Prevents infinity, NaN, and extreme values.
        """
        if v is None:
            return None
        
        # Block infinity and NaN
        if math.isinf(v):
            info_logger.error(f"Received infinite value for {info.field_name}")
            raise ValueError(f'{info.field_name} cannot be infinity')
        
        if math.isnan(v):
            info_logger.error(f"Received NaN value for {info.field_name}")
            raise ValueError(f'{info.field_name} cannot be NaN')
        
        # Must be positive
        if v <= 0:
            info_logger.error(f"Received non-positive value for {info.field_name}: {v}")
            raise ValueError(f'{info.field_name} must be positive')
        
        # Reasonable upper bound (prevent overflow)
        if v > 1_000_000:
            info_logger.error(f"Received excessively high value for {info.field_name}: {v}")
            raise ValueError(
                f'{info.field_name} exceeds maximum allowed value'
            )
        
        # Minimum price validation (prevent dust/spam)
        if v < 0.00000001:  # 1 satoshi equivalent
            info_logger.error(f"Received excessively low value for {info.field_name}: {v}")
            raise ValueError(
                f'{info.field_name} is too small'
            )
        
        return v
    
    @field_validator('qty')
    @classmethod
    def validate_qty_range(cls, v: Optional[float]) -> Optional[float]:
        """
        Validate quantity is reasonable percentage.
        
        Security: Prevents negative values, overflow, and extreme percentages.
        """
        if v is None:
            return None
        
        # Block infinity and NaN
        if math.isinf(v):
            info_logger.error("Received infinite quantity value")
            raise ValueError('Quantity cannot be infinity')
        
        if math.isnan(v):
            info_logger.error("Received NaN quantity value")
            raise ValueError('Quantity cannot be NaN')
        
        # Percentage range validation (0)
        if v < 0:
            info_logger.error(f"Received negative quantity value: {v}")
            raise ValueError('Quantity cannot be negative')
        
        # Minimum quantity validation (prevent dust trades)
        if v > 0 and v < 0.01:
            info_logger.error(f"Received excessively low quantity value: {v}")
            raise ValueError('Quantity too small')
        
        return v

class AlertBatch(BaseModel):
    alerts: List[Alert]
    
    @field_validator('alerts')
    @classmethod
    def validate_batch_size(cls, v: List[Alert]) -> List[Alert]:
        """
        Validate batch size to prevent DOS attacks.
        
        Security: Limits batch size to prevent resource exhaustion.
        """
        if not v or len(v) == 0:
            info_logger.error("Received empty alert batch")
            raise ValueError('Alert batch cannot be empty')
        
        # Maximum batch size (aka how many alerts can come from the system at once - max should be based on max number of assets per system)
        if len(v) > MAX_BATCH_SIZE:
            info_logger.error(f"Received overly large alert batch: {len(v)} alerts")
            raise ValueError(
                f'Alert batch exceeds maximum size'
            )
        
        return v


class TaskPriority(Enum):
    EXIT = 1      # Highest priority
    OPEN = 2      # Lower priority
    MONITOR = 3   # Lowest priority

@dataclass
class TradingTask:
    system_id: str
    alert: Alert
    priority: TaskPriority
    created_at: float
    api_key: Optional[str] = None  # User's Hyperliquid API key
    wallet_address: Optional[str] = None  # User's wallet address

class AsyncTradingBot:
    """Async version of TradingBot with proper concurrency handling"""
    
    def __init__(self, system_id: str, api_key: Optional[str] = None, wallet_address: Optional[str] = None):
        """
        Initialize AsyncTradingBot with optional user-specific credentials.
        
        Args:
            system_id: Trading system identifier (Magnetar or BMD)
            api_key: Optional Hyperliquid API key
            wallet_address: Optional wallet address
        """
        self.system_id = system_id
        self.config = SYSTEM_CONFIGS[system_id]
        
        if not self.config:
            raise ValueError(f"Unknown system ID: {system_id}. Available systems: {list(SYSTEM_CONFIGS.keys())}")
        self._lock = asyncio.Lock()  # Prevent concurrent position modifications
        self._order_tracking = {}  # Track active orders per asset
        
        # Use provided credentials
        private_key = api_key
        self.address = wallet_address
        
        if not private_key:
            info_logger.error("Missing API key for trading bot initialization")
            raise ValueError("API key required")
        if not self.address:
            info_logger.error("Missing wallet address for trading bot initialization")
            raise ValueError("Wallet address required")
        
        # Initialize HL connection
        self.account = Account.from_key(private_key)
        self.info = Info(constants.MAINNET_API_URL, skip_ws=True)
        self.exchange = Exchange(self.account, constants.MAINNET_API_URL)
        
        # Asset metadata
        self.asset_meta: Dict[str, any] = {m['name']: m for m in self.info.meta()['universe']}
        
        # Log initialization (truncate wallet for security)
        wallet_display = f"{self.address[:8]}..." if self.address else "N/A"
        info_logger.info(f"AsyncTradingBot initialized for {system_id} (wallet: {wallet_display})")
    
    # HELPER METHODS
    def _get_asset_info(self, asset: str) -> dict:
        """Get asset metadata"""
        asset_upper = asset.upper()
        if asset_upper in self.asset_meta:
            return self.asset_meta[asset_upper]
        info_logger.error(f"Asset metadata not found for {asset_upper}")
        raise ValueError(f"Could not find metadata for asset {asset}")
    
    def _format_price(self, price: float, asset: str) -> str:
        """
        Format price according to Hyperliquid tick size requirements.
        
        Rules:
        1. Integer prices are always valid (regardless of significant figures)
        2. Maximum 5 significant figures for non-integer prices
        3. Maximum decimal places: 6 - szDecimals (for perps)
        
        Both rules 2 and 3 must be satisfied simultaneously.
        """
        try:
            asset_info = self._get_asset_info(asset)
            sz_decimals = asset_info.get('szDecimals', 3)
            
            # Check if price is an integer
            if price == int(price):
                return str(int(price))
            
            # Convert to string for analysis
            price_str = f"{price:.15f}".rstrip('0').rstrip('.')
            
            if '.' not in price_str:
                return price_str
            
            integer_part, decimal_part = price_str.split('.')
            
            # Count significant figures (excluding leading zeros)
            if integer_part == '0' or integer_part == '':
                # For numbers < 1, count only non-zero digits in decimal part
                significant_figures = len(decimal_part.lstrip('0'))
            else:
                # For numbers >= 1, count all digits in integer part + decimal part
                significant_figures = len(integer_part.lstrip('0')) + len(decimal_part)
            
            # Maximum allowed decimal places: 6 - szDecimals
            max_decimal_places = max(0, 6 - sz_decimals)
            
            # Apply both constraints
            result_int = integer_part
            result_dec = decimal_part
            
            # Rule 1: Limit to 5 significant figures
            if significant_figures > 5:
                if integer_part == '0' or integer_part == '':
                    # For 0.xxxxx numbers, keep first 5 non-zero digits
                    leading_zeros = len(decimal_part) - len(decimal_part.lstrip('0'))
                    result_dec = decimal_part[:leading_zeros + 5]
                else:
                    # For x.xxxxx numbers, total 5 sig figs
                    int_sig_figs = len(integer_part.lstrip('0'))
                    remaining_sig_figs = 5 - int_sig_figs
                    if remaining_sig_figs > 0:
                        result_dec = decimal_part[:remaining_sig_figs]
                    else:
                        # Integer part already has 5+ sig figs, no decimals allowed
                        result_dec = ''
            
            # Rule 2: Limit to max_decimal_places
            if len(result_dec) > max_decimal_places:
                result_dec = result_dec[:max_decimal_places]
            
            # Construct final result
            if result_dec:
                # Only strip trailing zeros from decimal portion (This avoids removal of a 0 from an already correct integer part)
                result_dec = result_dec.rstrip('0')
                if result_dec:
                    result = f"{result_int}.{result_dec}"
                else:
                    result = result_int
            else:
                result = result_int
            
            # Ensure we have a valid number
            if result == '' or result == '.':
                result = '0'
            
            return result
            
        except Exception as e:
            info_logger.warning(f"Price formatting error for {asset}: {e}, using fallback")
            # Fallback: truncate to 5 sig figs
            return f"{price:.5g}"
    
    def _format_size(self, size: float, sz_decimals: int) -> str:
        """Format size according to HL requirements"""
        value_dec = Decimal(str(size))
        step_dec = Decimal('1e-' + str(sz_decimals))
        formatted_dec = (value_dec // step_dec) * step_dec
        return f"{formatted_dec.normalize()}"
    
    def get_portfolio_value(self) -> float:
        """Get portfolio value for this system's account/subaccount"""
        try:
            user_state = self.info.user_state(self.address)
            # return float(user_state['marginSummary']['accountValue'])
            account_value = float(user_state['marginSummary']['accountValue'])
            
            # Sum unrealizedPnL from all open positions in assetPositions
            asset_positions = user_state['assetPositions']
            total_unrealized_pnl = sum(float(pos['position']['unrealizedPnl']) for pos in asset_positions if float(pos['position']['szi']) != 0)
            realized_portfolio = (account_value - total_unrealized_pnl) 
            return realized_portfolio
        except Exception as e:
            info_logger.error(f"Failed to fetch portfolio value for {self.system_id}: {e}")
            return 0.0

    def _get_max_leverage(self, ticker: str) -> int:
        """Get maximum leverage for a ticker"""
        tickers_20 = ["SOL", "XRP"]
        tickers_10 = ["AAVE", "ADA", "APT", "ARB", "AVAX", "BCH", "BNB", "KBONK", "BONK", "CRV", "DOGE", "DOT", "ENA", "FARTCOIN", "HYPE", "INJ", "JUP", "LDO", "LINK", "LTC", "MKR", "NEAR", "ONDO", "OP", "KPEPE", "PEPE", "POPCAT", "SEI", "KSHIB", "SHIB", "SUI", "TIA", "TON", "TRUMP", "TRX", "UNI", "WIF", "WLD"]
        tickers_5 = ["AI16Z", "AIXBT", "ALGO", "ALT", "APE", "AR", "ATOM", "BABY", "BERA", "BIGTIME", "BIO", "BLUR", "BRETT", "CFX", "COMP", "DYDX", "EIGEN", "ENS", "ETC", "ETHFI", "FET", "FIL", "KFLOKI", "FLOKI", "FXS", "GMT", "GMX", "GOAT", "GRASS", "GRIFFAIN", "HBAR", "IMX", "IO", "JTO", "KAITO", "ME", "MELANIA", "MEME", "MEW", "MNT", "MOODENG", "MORPHO", "MOVE", "KNEIRO", "NEIRO", "NEIROETH", "NEO", "NXPC", "OM", "ORDI", "PAXG", "PENDLE", "PENGU", "PNUT", "POL", "PROMPT", "PYTH", "RENDER", "RUNE", "S", "SAGA", "SAND", "SCR", "SOPH", "SPX", "STRK", "STX", "SUSHI", "TAO", "TURBO", "USUAL", "VINE", "VIRTUAL", "W", "XLM", "ZEN", "ZEREBRO", "ZK", "ZRO"]
        tickers_3 = ["ACE", "ANIME", "ARK", "BANANA", "BLAST", "BOME", "BSV", "CAKE", "CELO", "CHILLGUY", "KDOGS", "DOGS", "DODO", "DYM", "FTT", "GALA", "GAS", "HMSTR", "HYPER", "INIT", "IOTA", "IP", "KAS", "LAUNCHCOIN", "LAYER", "KLUNC", "LUNC", "MANTA", "MAV", "MAVIA", "MERL", "MINA", "NIL", "NOT", "OGN", "OMNI", "PEOPLE", "POLYX", "PUMP", "PURR", "REQ", "REZ", "RSR", "RESOLV", "SNX", "STG", "SUPER", "SYRUP", "TNSR", "TRB", "TST", "UMA", "USTC", "VVV", "WCT", "XAI", "YGG", "ZETA", "ZORA", "LINEA"]
        
        ticker = ticker.upper()
        if ticker in tickers_3:
            return 3
        elif ticker in tickers_5:
            return 5
        elif ticker in tickers_10:
            return 10
        elif ticker in tickers_20:
            return 20
        elif ticker == "ETH":
            return 25
        elif ticker == "BTC":
            return 40
        else:
            return 5  # default
    
    def _calculate_position_parameters(self, alert: Alert) -> dict:
        """Calculate position size, leverage, and margin"""
        portfolio_value = self.get_portfolio_value()
        
        if portfolio_value <= 0:
            raise ValueError(f"Invalid portfolio value: {portfolio_value}")
        
        # Apply system-specific risk multiplier
        effective_portfolio = portfolio_value * self.config["system_weight"]
        
        # Calculate position size
        position_size = effective_portfolio * (alert.qty / 100.0)
        max_margin_per_pos = effective_portfolio / self.config["max_assets"]
        
        if max_margin_per_pos <= 0:
            raise ValueError("Maximum margin is zero or negative")
        
        leverage = math.ceil(position_size / max_margin_per_pos)
        if leverage == 0:
            leverage = 1
        
        # Check maximum leverage for this ticker
        max_leverage = self._get_max_leverage(alert.ticker)
        if leverage > max_leverage:
            leverage = max_leverage
            position_size = max_margin_per_pos * leverage
        
        applied_margin = position_size / leverage
        quantity = position_size / alert.price
        
        asset_info = self._get_asset_info(alert.ticker)
        formatted_quantity = self._format_size(quantity, asset_info['szDecimals'])
        
        # Calculate original risk if stop loss is provided (for constant risk recalculation)
        original_total_risk = None
        if alert.stop_loss:
            risk_per_unit = abs(alert.price - alert.stop_loss)
            original_total_risk = risk_per_unit * float(formatted_quantity)
        
        return {
            "asset": alert.ticker.upper(),
            "leverage": leverage,
            "margin": round(applied_margin, 2),
            "quantity": float(formatted_quantity),
            "position_size": position_size,
            # Data for constant risk recalculation during retries
            "original_entry_price": alert.price,
            "stop_loss": alert.stop_loss,
            "original_total_risk": original_total_risk,
            "target_position_size_usd": position_size,
            "sz_decimals": asset_info['szDecimals'],
            "max_margin_per_pos": max_margin_per_pos,
            "max_leverage": max_leverage,
        }
    
    
    def _recalculate_quantity_for_constant_risk(
        self, 
        params: dict, 
        new_price: float, 
        is_buy: bool,
        price_drift_threshold: float = PRICE_DRIFT_THRESHOLD_FOR_RISK_RECALC
    ) -> Tuple[float, bool, Optional[dict]]:
        """
        Recalculate quantity to maintain CONSTANT RISK (1R) when entry price drifts.
        
        Risk = (Entry Price - Stop Loss) × Quantity
        We want: New Risk = Original Risk
        
        If no stop loss -> return error
        If recalculated quantity exceeds margin limits, caps at max allowed.
        
        Args:
            params: Position parameters from _calculate_position_parameters
            new_price: Current market price
            is_buy: True if long position, False if short
            price_drift_threshold: % threshold to trigger recalculation (default 1.5%)
        
        Returns:
            tuple: (new_quantity, was_recalculated, recalc_info_dict)
        """

        # NOTE - IMPORTANT - For some systems the risk is 0.2R or 0.6R instead of 1R
        # However, this calculation looks at the Risk $ amount so it will work
        # Just relevant to not get sucked into fixating on 1R, but rather keeping same given $ risk amount

        original_price = params.get('original_entry_price', new_price)
        original_quantity = params['quantity']
        stop_loss = params.get('stop_loss')
        sz_decimals = params.get('sz_decimals', 3)
        
        # Calculate price drift
        if original_price > 0:
            price_drift = abs(new_price - original_price) / original_price
        else:
            price_drift = 0
        
        # Check if drift exceeds threshold
        if price_drift <= price_drift_threshold:
            return original_quantity, False, None
        
        log_prefix = f"[{self.system_id} RISK RECALC]"
        
        # If no stop loss provided, fall back to maintaining USD exposure
        if not stop_loss:
            target_position_size_usd = params.get('target_position_size_usd', 0)
            if target_position_size_usd <= 0:
                return original_quantity, False, None
            
            new_quantity = target_position_size_usd / new_price
            formatted_quantity = float(self._format_size(new_quantity, sz_decimals))
            
            info_logger.info(
                f"{log_prefix} No SL provided. Maintaining USD exposure: "
                f"${target_position_size_usd:.2f} / ${new_price:.2f} = {formatted_quantity}"
            )
            
            recalc_info = {
                "method": "usd_exposure",
                "price_drift_pct": price_drift * 100,
                "original_quantity": original_quantity,
                "new_quantity": formatted_quantity,
            }
            return formatted_quantity, True, recalc_info
        
        # Calculate original risk in dollars
        original_risk_per_unit = abs(original_price - stop_loss)
        original_total_risk = params.get('original_total_risk')
        
        if original_total_risk is None:
            original_total_risk = original_risk_per_unit * original_quantity
        
        # Calculate new quantity to maintain same total risk
        new_risk_per_unit = abs(new_price - stop_loss)
        
        if new_risk_per_unit <= 0:
            info_logger.warning(
                f"{log_prefix} Invalid risk calculation: Entry ${new_price:.2f}, "
                f"SL ${stop_loss:.2f}. Keeping original quantity."
            )
            return original_quantity, False, None
        
        new_quantity = original_total_risk / new_risk_per_unit
        
        # Cap quantity if it exceeds margin limits
        max_margin = params.get('max_margin_per_pos', float('inf'))
        max_leverage = params.get('max_leverage', 5)
        max_position_size = max_margin * max_leverage
        max_quantity = max_position_size / new_price if new_price > 0 else new_quantity
        
        quantity_was_capped = False
        if new_quantity > max_quantity:
            info_logger.warning(
                f"{log_prefix} Recalculated quantity {new_quantity:.6f} exceeds max "
                f"{max_quantity:.6f} (margin limit). Capping."
            )
            new_quantity = max_quantity
            quantity_was_capped = True
        
        formatted_quantity = float(self._format_size(new_quantity, sz_decimals))
        
        # Calculate what the new risk will be (may differ slightly due to formatting)
        actual_new_risk = new_risk_per_unit * formatted_quantity
        
        info_logger.info(
            f"{log_prefix} CONSTANT RISK recalc triggered (drift: {price_drift*100:.2f}%)\n"
            f"  Original: {original_quantity} @ ${original_price:.2f} "
            f"(risk/unit: ${original_risk_per_unit:.2f}, total: ${original_total_risk:.2f})\n"
            f"  New: {formatted_quantity} @ ${new_price:.2f} "
            f"(risk/unit: ${new_risk_per_unit:.2f}, total: ${actual_new_risk:.2f})"
            f"{' [CAPPED]' if quantity_was_capped else ''}"
        )
        
        recalc_info = {
            "method": "constant_risk",
            "price_drift_pct": price_drift * 100,
            "original_quantity": original_quantity,
            "new_quantity": formatted_quantity,
            "original_risk": original_total_risk,
            "new_risk": actual_new_risk,
            "was_capped": quantity_was_capped,
        }
        
        return formatted_quantity, True, recalc_info


    def _asset_has_existing_position(self, ticker: str) -> bool:
        """Check if asset has existing position"""
        try:
            user_state = self.info.user_state(self.address)
            positions = user_state.get('assetPositions', [])
            
            for position in positions:
                if position["position"]["coin"] == ticker.upper():
                    position_size = float(position["position"]["szi"])
                    return abs(position_size) > 0.0001  # Consider positions > 0.0001 as existing
            
            return False
        except Exception as e:
            info_logger.error(f"Error checking existing position for {ticker}: {e}")
            return False
    
    def _get_asset_mark_price(self, asset: str) -> float:
        """Get current mark price for asset"""
        try:
            all_mids = self.info.all_mids()
            if not isinstance(all_mids, dict):
                raise Exception(f"API returned invalid all_mids: {all_mids}")
            return float(all_mids.get(asset.upper(), 0.0))
        except Exception as e:
            info_logger.error(f"Failed to get mark price for {asset}: {e}")
            return 0.0
    
    def close_position_by_asset(self, asset: str):
        """Close position for specific asset with verification and retry logic"""
        asset = asset.upper()
        log_prefix = f"[{self.config['trade_log_prefix']} CLEANUP/CLOSE {asset}]"
        
        try:
            # 1. Cancel all open orders for the asset
            open_orders = self.info.open_orders(self.address)
            asset_orders = [o for o in open_orders if o['coin'] == asset]


            if asset_orders:
                for order in asset_orders:
                    self.exchange.cancel(asset, order['oid'])
                    info_logger.info(f"{log_prefix} Cancelled previous order {order['oid']} for {asset}.")
                
                time.sleep(0.5)  # Give time for cancellations to process
            
            # 2. Get initial position to close
            user_state = self.info.user_state(self.address)
            if not isinstance(user_state, dict):
                raise Exception(f"API returned invalid user_state: {user_state}")

            position = next((p for p in user_state.get('assetPositions', []) if p['position']['coin'] == asset), None)

            if not position or float(position['position']['szi']) == 0:
                info_logger.info(f"{log_prefix} No open position found to close.")
                return
            
            original_pos_size = float(position['position']['szi'])
            is_buy_to_close = original_pos_size < 0  # If position is short (-), buy to close
            target_quantity = abs(original_pos_size)
            
            asset_info = self._get_asset_info(asset)
            formatted_quantity = self._format_size(target_quantity, asset_info['szDecimals'])
            
            # 3. Place initial limit order to close position
            current_price = self._get_asset_mark_price(asset)
            formatted_price = self._format_price(current_price, asset)
            order_result = self.exchange.order(
                asset, 
                is_buy_to_close, 
                float(formatted_quantity), 
                float(formatted_price), 
                {"limit": {"tif": "Gtc"}}, 
                True  # reduce_only flag
            )
        
            # Handle the below
            # Initial order result: {'status': 'err', 'response': 'User or API Wallet 0x1e468337ab4d8cfe50b0f762a388d083ca8efafe does not exist.'}

            # Handle both successful order responses and error strings
            # Begin with checking for "err" status, only look at normal expected response afterwards if no error
            order_id = None

            if isinstance(order_result, dict) and order_result.get('status') == 'err':
                error_message = order_result.get('response', 'Unknown error')
                info_logger.error(f"{log_prefix} Initial order placement failed with error: {error_message}")
                raise Exception(f"Order placement failed: {error_message}")

            if isinstance(order_result, dict):
                try:
                    order_id = order_result.get('response', {}).get('data', {}).get('statuses', [{}])[0].get('resting', {}).get('oid')
                except (KeyError, IndexError, TypeError):
                    order_id = None
            elif isinstance(order_result, str):
                # API returned an error string
                info_logger.error(f"{log_prefix} Initial order placement returned error: {order_result}")
                raise Exception(f"Order placement failed: {order_result}")
            else:
                # Unexpected type
                info_logger.warning(f"{log_prefix} Unexpected retry_order_result type: {type(retry_order_result)}, value: {retry_order_result}")
                new_order_id = None
            
            initial_message = f"{log_prefix} Initial close order placed: {formatted_quantity} at {formatted_price} (Order ID: {order_id})"
            info_logger.info(initial_message)
            
            # 4. Monitor position closure with retry logic
            max_retries = 7
            retry_count = 0
            filled = False
            slippage = 0.0

            while not filled: 
                time.sleep(20)  # Wait for order processing
                retry_count += 1
                
                try:
                    # Check current position
                    current_user_state = self.info.user_state(self.address)
                    if not isinstance(current_user_state, dict):
                        info_logger.warning(f"{log_prefix} API returned invalid user_state during retry: {current_user_state}")
                        continue  # Skip this retry iteration
                    current_position = next((p for p in current_user_state.get('assetPositions', []) 
                                           if p['position']['coin'] == asset), None)
                    
                    current_position_size = 0.0
                    if current_position and current_position['position']['szi']:
                        current_position_size = float(current_position['position']['szi'])
                    
                    info_logger.info(f"{log_prefix} Retry {retry_count}: Current position size: {current_position_size}")
                    
                    # Check if position is fully closed
                    if abs(current_position_size) < 0.0001:  # Essentially zero
                        success_message = f"{log_prefix} SUCCESS | Position fully closed. Final size: {current_position_size}"
                        info_logger.info(success_message)
                        
                        filled = True # technically irrelevant since we break the loop via return - I suppose this is just for clarity
                        
                        log_data = {
                            "system_id": self.system_id,
                            "asset": asset,
                            "position_side": "sell" if is_buy_to_close else "buy",
                            "orderId": order_id,                                        # orderid will be None if closed on first retry (and possibly also in general)
                            # orderid things might be something we want to correct in the future, orderid itself is only overwritten further down in the retry loop

                            # "quantity": original_pos_size,
                            # "closedQuantity": formatted_quantity,
                            "exitPrice": formatted_price
                        }

                        trade_logger.info(f"Position fully closed for {self.system_id}", extra=log_data)
                        return
                    
                    # If still have position, cancel old order and place new one at current market price
                    info_logger.info(f"{log_prefix} Position not fully closed, placing retry order...")
                    
                    # Cancel previous order if it exists
                    if order_id:
                        try:
                            self.exchange.cancel(asset, order_id)
                            time.sleep(0.5)
                        except Exception as cancel_e:
                            info_logger.warning(f"{log_prefix} Could not cancel order {order_id}: {cancel_e}")
                    
                    # Calculate remaining quantity to close
                    remaining_quantity = abs(current_position_size)
                    formatted_remaining = self._format_size(remaining_quantity, asset_info['szDecimals'])
                    
                    # Get fresh market price for retry
                    new_market_price = self._get_asset_mark_price(asset)
                    new_formatted_price = self._format_price(new_market_price, asset)

                    if retry_count < max_retries:
                        # Place new order at current market price
                        retry_order_result = self.exchange.order(
                            asset,
                            is_buy_to_close,
                            float(formatted_remaining),
                            float(new_formatted_price),
                            {"limit": {"tif": "Gtc"}},
                            True  # reduce_only flag
                        )
                    else:
                        slippage += 0.002

                        # Use SDK's slippage price calculation (handles rounding and sig figs correctly)
                        aggressive_price = self.exchange._slippage_price(asset, is_buy_to_close, slippage, float(new_formatted_price))
                        # place increasingly aggressive limit order 
                        retry_order_result = self.exchange.order(
                            asset,
                            is_buy_to_close,
                            float(formatted_remaining),
                            aggressive_price,
                            {"limit": {"tif": "Gtc"}},
                            True  # reduce_only flag
                        )
                        info_logger.info(f"{log_prefix} Placed aggressive limit order for {formatted_remaining} {asset} at {aggressive_price} with slippage {slippage} (mark price: {new_formatted_price})")
                        # retry_order_result = self.exchange.order(
                        #     asset,
                        #     is_buy_to_close,
                        #     float(formatted_remaining),
                        #     float(new_formatted_price),
                        #     {"trigger": {"isMarket": True, "triggerPx": float(new_formatted_price), "tpsl" : "sl"}}, # market order
                        #     True  # reduce_only flag
                        # )
                        # info_logger.info(f"{log_prefix} Placed market close order for {formatted_remaining} {asset}")
 
                    
                    # Handle both successful order responses and error strings
                    new_order_id = None
                    if isinstance(retry_order_result, dict):
                        try:
                            new_order_id = retry_order_result.get('response', {}).get('data', {}).get('statuses', [{}])[0].get('resting', {}).get('oid')
                        except (KeyError, IndexError, TypeError):
                            new_order_id = None
                            
                    elif isinstance(retry_order_result, str):
                        # API returned an error string, log but continue
                        info_logger.warning(f"{log_prefix} Retry order placement returned error: {retry_order_result}")
                        new_order_id = None
                    else:
                        # Unexpected type
                        info_logger.warning(f"{log_prefix} Unexpected retry_order_result type: {type(retry_order_result)}, value: {retry_order_result}")
                        new_order_id = None
                    
                    if new_order_id:
                        order_id = new_order_id  # Update order_id for next iteration
                    
                    formatted_price = new_formatted_price  # Update for logging
                    
                    info_logger.info(f"{log_prefix} Retry order placed: {formatted_remaining} at {new_formatted_price} (Order ID: {new_order_id})")
                
                except Exception as e:
                    info_logger.error(f"{log_prefix} Monitor error on retry {retry_count}: {e}")
                    if "insufficient" in str(e).lower():
                        break
            
            # Cancel any remaining open orders for this asset to prevent orphans
            # During position close, we cancel all orders (including SL triggers) since the position is being exited
            try:
                remaining_orders = self.info.open_orders(self.address)
                asset_remaining = [o for o in remaining_orders if o['coin'] == asset]
                for order in asset_remaining:
                    try:
                        self.exchange.cancel(asset, order['oid'])
                        info_logger.info(f"{log_prefix} Cleaned up lingering order {order['oid']}")
                    except Exception:
                        info_logger.warning(f"{log_prefix} Failed to cancel lingering order {order['oid']}")
            except Exception as e:
                info_logger.warning(f"{log_prefix} Failed to check for lingering orders: {e}")

            # Final position check
            final_user_state = self.info.user_state(self.address)
            if not isinstance(final_user_state, dict):
                info_logger.warning(f"{log_prefix} API returned invalid user_state for final check: {final_user_state}")
                final_user_state = {'assetPositions': []}  # Treat as no positions
            final_position = next((p for p in final_user_state.get('assetPositions', []) 
                                 if p['position']['coin'] == asset), None)
            final_size = 0.0
            if final_position and final_position['position']['szi']:
                final_size = float(final_position['position']['szi'])
            
            if abs(final_size) < 0.0001:
                success_message = f"{log_prefix} SUCCESS | Position closed after retries. Final size: {final_size}"
                info_logger.info(success_message)
            else:
                warning_message = f"{log_prefix} WARNING | Position not fully closed after {max_retries} retries. Remaining: {final_size}"
                info_logger.warning(warning_message)
            
            log_data = {
                "system_id": self.system_id,
                "asset": asset,
                "status": "COMPLETED" if abs(final_size) < 0.0001 else "PARTIAL",
                "original_size": original_pos_size,
                "final_size": final_size,
                "retries": retry_count,
                "exit_price": formatted_price
            }
            trade_logger.info(f"Position close attempt completed for {self.system_id}", extra=log_data)
               
        except Exception as e:
            error_message = f"{log_prefix} FAILED: {e}"
            info_logger.error(error_message)
            log_data = {
                "system_id": self.system_id,
                "asset": asset,
                "status": "FAILED",
                "error": str(e)
            }
            trade_logger.error(f"Position close failed for {self.system_id}", extra=log_data)
            raise
    
    
    def _log_position_success(self, asset: str, alert: Alert, params: dict, formatted_price: str):
        """Log successful position opening"""
        log_data = {
            "system_id": self.system_id,
            "asset": asset,
            "position_side": alert.action,
            "status": "SUCCESS",
            # "margin": params['margin'],
            "leverage": params['leverage'],
            # "quantity": params['quantity'],
            "entryPrice": formatted_price,
            "stopLoss": alert.stop_loss
        }
        trade_logger.info(f"Position opened for {self.system_id}", extra=log_data)

    def _log_position_error(self, asset: str, alert: Alert, error: str, formatted_price: str):
        """Log position opening error"""
        log_data = {
            "system_id": self.system_id,
            "asset": asset,
            "position_side": alert.action,
            "entryPrice": formatted_price,
            "stopLoss": alert.stop_loss,
            "status": "FAILED",
            "error": error
        }
        trade_logger.error(f"Position open failed for {self.system_id}", extra=log_data)
    
    
    async def open_position_async(self, alert: Alert):
        """Async version of open_position with proper concurrency handling"""
        asset = alert.ticker.upper()
        log_prefix = f"[{self.config['trade_log_prefix']} OPEN {alert.action.upper()} {asset}]"
        formatted_price = str(alert.price)
        
        # Use lock to prevent concurrent position modifications for same asset
        async with self._lock:
            if asset in self._order_tracking:
                info_logger.warning(f"{log_prefix} Order already in progress for {asset}, skipping")
                return
            
            self._order_tracking[asset] = {"status": "opening", "start_time": time.time()}
        
        try:
            # Check existing position (run in thread pool to avoid blocking)
            existing_position = await asyncio.get_event_loop().run_in_executor(
                None, self._asset_has_existing_position, alert.ticker
            )
            
            if existing_position:
                info_logger.info(f"{log_prefix} Existing position detected, aborting")
                return
            
            # Cancel all open orders for the asset - in case some random open order is left for that asset -> clean slate
            open_orders = self.info.open_orders(self.address)
            asset_orders = [o for o in open_orders if o['coin'] == asset]
            
            if asset_orders:
                for order in asset_orders:
                    self.exchange.cancel(asset, order['oid'])
                    info_logger.info(f"{log_prefix} Cancelled previous order {order['oid']} for {asset}.")
                
                time.sleep(0.5)  # Give time for cancellations to process
            
            # Get portfolio value (async)
            portfolio_value = await asyncio.get_event_loop().run_in_executor(
                None, self.get_portfolio_value
            )
            
            if portfolio_value <= 0:
                info_logger.error(f"{log_prefix} Invalid portfolio value: ${portfolio_value:,.2f}")
                return
            
            # Calculate position parameters
            params = self._calculate_position_parameters(alert)
            
            # Set leverage (async)
            await asyncio.get_event_loop().run_in_executor(
                None, self.exchange.update_leverage, params['leverage'], asset, False
            )
            
            info_logger.info(f"{log_prefix} Set leverage to {params['leverage']}x")
          
            # Place initial order (async)
            is_buy = alert.action == "buy"
            formatted_price = self._format_price(alert.price, asset)

            # Place stop loss order prior to real order (for risk management)
            if alert.stop_loss:
                await self._place_stop_loss_async(asset, alert, params, is_buy)

            order_result = await asyncio.get_event_loop().run_in_executor(
                None, self.exchange.order, asset, is_buy, params['quantity'],
                float(formatted_price), {"limit": {"tif": "Gtc"}}
            )
            
            # Handle both successful order responses and error strings
            if isinstance(order_result, str):
                raise Exception(f"Entry order failed: {order_result}")
            elif not isinstance(order_result, dict):
                raise Exception(f"Unexpected order result type: {type(order_result)}")
            
            # If using main address as vault_address for exchange init then following error can occur:
            #  {'status': 'err', 'response': 'Vault not registered: given_address'}
            # This can result in this: 
            # Worker worker-Magnetar-0-1758961772 failed Magnetar:XRP: string indices must be integers, not 'str'
            # Therefore we need to explicitly check for 'status' key in response
            if order_result.get('status') == "err":
                info_logger.error(f"{log_prefix} Order error response: {order_result.get('response')}")
                raise Exception(f"Entry order failed: {order_result.get('response')}")
            
            order_status = order_result['response']['data']['statuses'][0]
            
            if 'error' in order_status:
                raise Exception(f"Entry order failed: {order_status['error']}")
            
            previous_order_id = order_status.get('resting', {}).get('oid')
            filled_data = order_status.get('filled', {})
            filled_order_id = filled_data.get('oid') if filled_data else None
            target_quantity = float(params['quantity'])
            expected_side = "long" if is_buy else "short"
                          
            # If order not immediately filled, start monitoring task
            if previous_order_id and not filled_order_id:
                formatted_price, target_quantity = await asyncio.create_task(self._monitor_order_fill(
                    asset, previous_order_id, params, is_buy
                ))
            elif filled_order_id:
                # Order reported as filled — but verify actual filled size matches intended size
                filled_sz = float(filled_data.get('totalSz', 0))
                avg_px = filled_data.get('avgPx')
                info_logger.info(f"{log_prefix} Initial order filled: {filled_sz}/{target_quantity} @ {avg_px}")
                
                if abs(filled_sz - target_quantity) < (target_quantity * 0.01):
                    # Fully filled
                    formatted_price = avg_px if avg_px else formatted_price
                else:
                    # Partial fill — continue monitoring for the remaining quantity
                    info_logger.warning(f"{log_prefix} Partial fill detected: got {filled_sz}, need {target_quantity}. Entering monitor loop for remainder.")
                    formatted_price, target_quantity = await asyncio.create_task(self._monitor_order_fill(
                        asset, None, params, is_buy
                    ))
            else:
                # No resting, no filled — check position directly
                user_state = await asyncio.get_event_loop().run_in_executor(
                    None, self.info.user_state, self.address
                )   
                positions = user_state.get('assetPositions', [])
                for position in positions:
                    if position["position"]["coin"] == asset:
                        formatted_price = float(position["position"]["entryPx"])
                        break
            
            # Post-fill verification: ensure position size matches intent and no orphan orders remain
            formatted_price = await self._verify_position_and_cleanup(
                asset, target_quantity, is_buy, expected_side, formatted_price, log_prefix
            )
            
            info_logger.info(f"{log_prefix} Position opened: {params['quantity']} at {formatted_price}")
            # Log success
            self._log_position_success(asset, alert, params, formatted_price)
            
        except Exception as e:
            self._log_position_error(asset, alert, str(e), formatted_price)
            raise
        finally:
            # Remove from tracking
            async with self._lock:
                self._order_tracking.pop(asset, None)
    

    async def _verify_position_and_cleanup(self, asset: str, target_quantity: float, 
                                            is_buy: bool, expected_side: str, 
                                            formatted_price, log_prefix: str):
        """
        Post-fill verification: ensure position size matches intent, cancel orphan orders,
        and correct any overfill caused by orders filling during cleanup.
        
        Returns the final formatted_price (entry price from position).
        """
        # 1. Check actual position size on exchange
        user_state = await asyncio.get_event_loop().run_in_executor(
            None, self.info.user_state, self.address
        )
        positions = user_state.get('assetPositions', [])
        current_position_size = 0.0
        entry_price = formatted_price
        
        for pos in positions:
            if pos["position"]["coin"] == asset:
                current_position_size = float(pos["position"]["szi"])
                entry_price = pos["position"].get("entryPx", formatted_price)
                break
        
        direction = "long" if current_position_size > 0 else "short"
        abs_size = abs(current_position_size)
        
        info_logger.info(f"{log_prefix} Post-fill verify: position={current_position_size}, target={target_quantity}, direction={direction}, expected={expected_side}")
        
        # 2. Cancel only non-trigger (limit) orders — preserve stop-loss trigger orders
        cancelled_any = False
        try:
            all_orders = await asyncio.get_event_loop().run_in_executor(
                None, self.info.frontend_open_orders, self.address
            )
            # Only cancel non-trigger limit orders; keep SL/TP trigger orders intact
            asset_limit_orders = [o for o in all_orders if o['coin'] == asset and not o.get('isTrigger', False)]
            
            if asset_limit_orders:
                info_logger.info(f"{log_prefix} Found {len(asset_limit_orders)} lingering limit order(s) after fill, cancelling (preserving trigger orders)...")
                for order in asset_limit_orders:
                    try:
                        await asyncio.get_event_loop().run_in_executor(
                            None, self.exchange.cancel, asset, order['oid']
                        )
                        info_logger.info(f"{log_prefix} Cancelled lingering limit order {order['oid']}")
                        cancelled_any = True
                    except Exception:
                        info_logger.warning(f"{log_prefix} Failed to cancel lingering order {order['oid']}")
        except Exception as e:
            info_logger.warning(f"{log_prefix} Failed to fetch open orders for cleanup: {e}")
        
        # 3. If we cancelled orders, re-check position — those orders may have filled in the meantime
        if cancelled_any:
            await asyncio.sleep(1)  # Brief pause for cancellations to settle
            
            user_state = await asyncio.get_event_loop().run_in_executor(
                None, self.info.user_state, self.address
            )
            positions = user_state.get('assetPositions', [])
            current_position_size = 0.0
            
            for pos in positions:
                if pos["position"]["coin"] == asset:
                    current_position_size = float(pos["position"]["szi"])
                    entry_price = pos["position"].get("entryPx", entry_price)
                    break
            
            abs_size = abs(current_position_size)
            info_logger.info(f"{log_prefix} Post-cancel re-check: position={current_position_size}, target={target_quantity}")
        
        # 4. Check if position is overfilled — if so, reduce back to target
        overfill_threshold = target_quantity * 1.01  # 1% tolerance
        if abs_size > overfill_threshold and direction == expected_side:
            excess = abs_size - target_quantity
            info_logger.warning(f"{log_prefix} Position OVERFILLED: {abs_size} vs target {target_quantity}, reducing by {excess}")
            
            try:
                asset_info = self._get_asset_info(asset)
                formatted_excess = self._format_size(excess, asset_info['szDecimals'])
                
                if float(formatted_excess) > 0:
                    # Close the excess (opposite direction, reduce_only)
                    reduce_is_buy = not is_buy
                    market_price = await asyncio.get_event_loop().run_in_executor(
                        None, self._get_asset_mark_price, asset
                    )
                    reduce_price = self.exchange._slippage_price(asset, reduce_is_buy, 0.005, market_price)
                    
                    reduce_result = await asyncio.get_event_loop().run_in_executor(
                        None, self.exchange.order, asset, reduce_is_buy, float(formatted_excess),
                        reduce_price, {"limit": {"tif": "Gtc"}}, True  # reduce_only
                    )
                    info_logger.info(f"{log_prefix} Placed reduce order for {formatted_excess} at {reduce_price}: {reduce_result}")
                    
                    # Wait and verify the reduction
                    await asyncio.sleep(5)
                    
                    user_state = await asyncio.get_event_loop().run_in_executor(
                        None, self.info.user_state, self.address
                    )
                    for pos in user_state.get('assetPositions', []):
                        if pos["position"]["coin"] == asset:
                            final_size = abs(float(pos["position"]["szi"]))
                            entry_price = pos["position"].get("entryPx", entry_price)
                            info_logger.info(f"{log_prefix} After reduction: position size={final_size}, target={target_quantity}")
                            break
                    
                    # Cancel any leftover reduce orders (non-trigger only, preserve SL)
                    all_orders = await asyncio.get_event_loop().run_in_executor(
                        None, self.info.frontend_open_orders, self.address
                    )
                    for order in [o for o in all_orders if o['coin'] == asset and not o.get('isTrigger', False)]:
                        try:
                            await asyncio.get_event_loop().run_in_executor(
                                None, self.exchange.cancel, asset, order['oid']
                            )
                        except Exception:
                            pass
                            
            except Exception as e:
                info_logger.error(f"{log_prefix} Failed to reduce overfilled position: {e}")
        
        return entry_price

    async def _monitor_order_fill(self, asset: str, order_id: str, params: dict, 
                                  is_buy: bool):
        """Monitor order filling in background without blocking"""
        log_prefix = f"[{self.config['trade_log_prefix']} MONITOR {asset}]"
        max_retries = 7
        retry_count = 0
        target_quantity = float(params['quantity'])
        expected_side = "long" if is_buy else "short"
        filled = False
        prev_order_id = order_id
        slippage = 0.0
        new_price = None
        recalc_result = None
                    
        while not filled: #retry_count < max_retries:
            await asyncio.sleep(40)  # Non-blocking sleep
            retry_count += 1
            info_logger.info(f"{log_prefix} Checking fill status (attempt {retry_count})")
            
            try:

                if retry_count < max_retries:
                    market_order = False
                    # Cancel previous order and place new one (async)
                    new_price, new_order_id, retry_fill, recalc_result = await self._retry_order_placement(
                        asset, prev_order_id, target_quantity, is_buy, market_order, 
                        slippage, expected_side, params)

                elif retry_count >= max_retries:
                    market_order = True
                    slippage += 0.002
                    slippage = min(slippage, 0.04) # 4% slippage max
                    # increasingly aggressive limit order to ensure position is taken
                    new_price, new_order_id, retry_fill, recalc_result = await self._retry_order_placement(
                        asset, prev_order_id, target_quantity, is_buy, market_order, 
                        slippage, expected_side, params)
                
                # Update target quantity if recalculation occurred
                if recalc_result and recalc_result.get('recalculated'):
                    target_quantity = recalc_result['new_quantity']
                    params['quantity'] = target_quantity

                filled = retry_fill
                if new_order_id is not None:
                    prev_order_id = new_order_id
            except Exception as e:
                info_logger.error(f"{log_prefix} Monitor error: {e}")
                if recalc_result and recalc_result.get('recalculated'):
                    target_quantity = recalc_result['new_quantity']
                    params['quantity'] = target_quantity
                if "insufficient" in str(e).lower():
                    break

        # Cancel remaining non-trigger limit orders for this asset to prevent orphans
        # Uses frontend_open_orders to distinguish limit orders from SL/TP trigger orders
        try:
            all_orders = await asyncio.get_event_loop().run_in_executor(
                None, self.info.frontend_open_orders, self.address
            )
            asset_limit_orders = [o for o in all_orders if o['coin'] == asset and not o.get('isTrigger', False)]
            for order in asset_limit_orders:
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, self.exchange.cancel, asset, order['oid']
                    )
                    info_logger.info(f"{log_prefix} Cleaned up lingering limit order {order['oid']}")
                except Exception:
                    info_logger.warning(f"{log_prefix} Failed to cancel lingering order {order['oid']}")
        except Exception as e:
            info_logger.warning(f"{log_prefix} Failed to check for lingering orders: {e}")

        return new_price, target_quantity
    
    async def _retry_order_placement(self, asset: str, old_order_id: str, 
                                   target_quantity: float, is_buy: bool, market_order: bool, 
                                   slippage: float, expected_side: str, params: dict = None):
        """Place retry order asynchronously"""
        log_prefix = f"[{self.config['trade_log_prefix']} RETRY {asset}]"
        recalc_result = None
        try:
            
            if old_order_id:
                # Cancel old order by ID
                await asyncio.get_event_loop().run_in_executor(
                    None, self.exchange.cancel, asset, old_order_id
                )
            
            # Sweep any stale non-trigger limit orders for this asset
            # Prevents order accumulation when order IDs are lost due to parsing failures
            try:
                all_orders = await asyncio.get_event_loop().run_in_executor(
                    None, self.info.frontend_open_orders, self.address
                )
                stale_limit_orders = [o for o in all_orders if o['coin'] == asset and not o.get('isTrigger', False)]
                for stale_order in stale_limit_orders:
                    try:
                        await asyncio.get_event_loop().run_in_executor(
                            None, self.exchange.cancel, asset, stale_order['oid']
                        )
                        info_logger.info(f"{log_prefix} Cancelled stale limit order {stale_order['oid']}")
                    except Exception:
                        pass
            except Exception as e:
                info_logger.warning(f"{log_prefix} Failed to sweep stale orders: {e}")
            
            await asyncio.sleep(1)  # Brief pause

            # check after the canceling of prior order if any order has been filled
            # Must have to prevent accidentally double filling an order as it was filled between the last order check and the cancel statement
            user_state = await asyncio.get_event_loop().run_in_executor(
                None, self.info.user_state, self.address
            )
            
            positions = user_state.get('assetPositions', [])
            current_position_size = 0.0
            
            for position in positions:
                if position["position"]["coin"] == asset:
                    current_position_size = float(position["position"]["szi"])
                    break
            

            # direction = "long" if current_position_size > 0 else "short"

            # Zero position maps to "short". If expected_side is "short", 
            # the direction check passes but the size check catches it. 
            # Not a bug, but misleading.
            # Instead we go by system -> BMD short, Magnetar long
            direction = "short" if self.config['trade_log_prefix'] == "BMD" else "long"
            
            # Check if sufficiently filled
            # After detecting fill in _retry_order_placement:
            if (direction == expected_side and 
                abs(abs(current_position_size) - target_quantity) < (target_quantity * 0.01)):
                
                # Get actual entry price from position data already in hand
                entry_price = None
                for pos in positions:
                    if pos["position"]["coin"] == asset:
                        entry_price = pos["position"].get("entryPx")
                        break
                
                info_logger.info(f"Retry: Position filled successfully: {current_position_size}")
                return entry_price, None, True, recalc_result  # return actual price instead of None
            
            # Calculate remaining and place new order if needed
            current_amount = abs(current_position_size) if direction == expected_side else 0.0
            remaining = target_quantity - current_amount
            
            if remaining <= target_quantity * 0.01:
                # Get actual entry price from position data already in hand
                entry_price = None
                for pos in positions:
                    if pos["position"]["coin"] == asset:
                        entry_price = pos["position"].get("entryPx")
                        break

                info_logger.info(f"Retry: Position essentially complete")
                return entry_price, None, True, recalc_result
            
            # Get current market price and place new order
            market_price = await asyncio.get_event_loop().run_in_executor(
                None, self._get_asset_mark_price, asset
            )
            
            # Recalculate quantity for constant risk if params available
            if params:
                new_quantity, was_recalculated, recalc_info = self._recalculate_quantity_for_constant_risk(
                    params, market_price, is_buy
                )
                
                if was_recalculated:
                    info_logger.info(
                        f"{log_prefix} Target quantity updated: {target_quantity:.6f} → {new_quantity:.6f} "
                        f"(maintaining constant risk)"
                    )
                    target_quantity = new_quantity
                    recalc_result = {
                        'recalculated': True,
                        'new_quantity': new_quantity,
                        'info': recalc_info
                    }
            
            # Calculate remaining and place new order if needed
            remaining = target_quantity - current_amount
            
            if remaining <= target_quantity * 0.01:
                info_logger.info(f"{log_prefix} Position essentially complete")
                return None, None, True, recalc_result

            formatted_price = self._format_price(market_price, asset)
            asset_info = self._get_asset_info(asset)
            formatted_remaining = self._format_size(remaining, asset_info['szDecimals'])
            
            if not market_order:
                order_result = await asyncio.get_event_loop().run_in_executor(
                    None, self.exchange.order, asset, is_buy, float(formatted_remaining),
                    float(formatted_price), {"limit": {"tif": "Gtc"}}
                )
            else:
                # Use SDK's slippage price calculation (handles rounding and sig figs correctly)
                aggressive_price = self.exchange._slippage_price(asset, is_buy, slippage, float(formatted_price))

                # place increasingly aggressive limit order 
                order_result = await asyncio.get_event_loop().run_in_executor(
                    None, self.exchange.order, asset, is_buy, float(formatted_remaining),
                    aggressive_price, {"limit": {"tif": "Gtc"}}, 
                )
                info_logger.info(f"{log_prefix} Placed aggressive limit order for {formatted_remaining} {asset} at {aggressive_price} with slippage {slippage} (mark price: {formatted_price})")

            # Handle order result
            if isinstance(order_result, str):
                info_logger.error(f"{log_prefix} Order failed with error: {order_result}")
                return None, None, False, recalc_result
            
            if not isinstance(order_result, dict):
                info_logger.error(f"{log_prefix} Unexpected order result type: {type(order_result)}")
                return None, None, False, recalc_result
            
            # Check for API error response
            if order_result.get('status') == 'err':
                info_logger.error(f"{log_prefix} Order error: {order_result.get('response')}")
                return None, None, False, recalc_result
            
            # Extract order ID
            try:
                order_status = order_result['response']['data']['statuses'][0]
                previous_order_id = order_status.get('resting', {}).get('oid')
                
                if not previous_order_id:
                    filled_data = order_status.get('filled', {})
                    filled_oid = filled_data.get('oid')
                    if filled_oid:
                        filled_sz = float(filled_data.get('totalSz', 0))
                        avg_px = filled_data.get('avgPx')
                        # Need to double check if size is correct, possible that HL returns "filled" oid but only for partial size
                        # so we check if filled size is within 1% of remaining quantity to confirm it was genuinely filled
                        if abs(filled_sz - float(formatted_remaining)) < (float(formatted_remaining) * 0.01):
                            info_logger.info(f"{log_prefix} Order filled immediately: ID:{filled_oid} ({filled_sz} @ {avg_px})")
                            return avg_px, filled_oid, True, recalc_result
                        else:
                            info_logger.warning(f"{log_prefix} Partial immediate fill: {filled_sz}/{formatted_remaining} @ {avg_px}, will retry remaining on next iteration")
                            return formatted_price, None, False, recalc_result
                    else:
                        info_logger.warning(f"{log_prefix} No resting or filled OID in response: {order_status}")
                        return formatted_price, None, False, recalc_result
                            
                info_logger.info(f"{log_prefix} Retry order placed: {formatted_remaining} at {formatted_price} (ID: {previous_order_id})")
                return formatted_price, previous_order_id, False, recalc_result
                ## Returning the ID makes it possible to cancle the prior order on the next run through
                
            except (KeyError, IndexError, TypeError) as parse_error:
                info_logger.error(f"{log_prefix} Failed to parse order response: {parse_error}\nOrder result: {order_result}")
                return formatted_price, None, False, recalc_result

        except Exception as e:
            info_logger.error(f"Retry order failed: {e}")
            return None, None, False, None

    async def _place_stop_loss_async(self, asset: str, alert: Alert, params: dict, is_buy: bool):
        """Place stop loss order asynchronously"""
        formatted_stop_price = self._format_price(alert.stop_loss, asset)
        stop_loss_is_buy = not is_buy
        trigger = {"triggerPx": float(formatted_stop_price), "isMarket": True, "tpsl": "sl"}
        stop_order_type = {"trigger": trigger}
        
        await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.order, asset, stop_loss_is_buy, float(params['quantity']),
            float(formatted_stop_price), stop_order_type, True
        )
        
        info_logger.info(f"Stop loss placed at {formatted_stop_price}")
    
    async def close_position_async(self, asset: str):
        """Async version of position closing - runs in parallel without global lock"""
        asset = asset.upper()
        log_prefix = f"[{self.config['trade_log_prefix']} CLOSE {asset}]"
        
        try:
            # Run close operation in executor without global lock for parallel execution
            # Each asset closure is independent and can run concurrently
            await asyncio.get_event_loop().run_in_executor(
                None, self.close_position_by_asset, asset
            )
            
            info_logger.info(f"{log_prefix} Position closed successfully")
            
        except Exception as e:
            info_logger.error(f"{log_prefix} Close failed: {e}")
            raise
 
class ParallelBotManager:
    """Enhanced bot manager with parallel processing capabilities"""
    
    def __init__(self, max_concurrent_tasks: int = 8):
        self.bots: Dict[str, AsyncTradingBot] = {}
        self.task_queue = asyncio.Queue()
        self.max_concurrent_tasks = max_concurrent_tasks     # Maximum 7 tasks processing at once
        self.worker_semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self._active_workers = set()
        self._worker_shutdown_event = asyncio.Event()
        self._workers_started = False
    
    def get_bot(self, system_id: str, api_key: Optional[str] = None, wallet_address: Optional[str] = None) -> AsyncTradingBot:
        """
        Get or create async bot for system ID and user credentials.
        
        Creates unique bot instances per user by keying on system_id + wallet_address.
        Falls back to environment variables if credentials not provided (backward compatibility).
        
        Args:
            system_id: Trading system identifier (Magnetar or BMD)
            api_key: Optional Hyperliquid API key
            wallet_address: Optional wallet address
        
        Returns:
            AsyncTradingBot instance for the specified user
        """
        # Create unique key for this user's bot instance
        wallet = wallet_address
        bot_key = f"{system_id}:{wallet}"
        
        if bot_key not in self.bots:
            self.bots[bot_key] = AsyncTradingBot(system_id, api_key=api_key, wallet_address=wallet_address)
        
        return self.bots[bot_key]
    

    async def _create_worker(self, worker_id: str):
        """
        Create a worker that processes tasks one at a time with rate limit awareness.
        
        Workers will continuously pull from the queue until shutdown signal is received.
        The semaphore ensures no more than max_concurrent_tasks are executing simultaneously.
        """
        self._active_workers.add(worker_id)
        
        try:
            while not self._worker_shutdown_event.is_set():
                try:
                    # Wait for task with timeout
                    task = await asyncio.wait_for(self.task_queue.get(), timeout=30.0)
                    
                    # Acquire semaphore - blocks if 7 tasks already running
                    async with self.worker_semaphore:
                        start_time = time.time()
                        bot = self.get_bot(task.system_id, api_key=task.api_key, wallet_address=task.wallet_address)
                        
                        try:
                            if task.alert.action == "exit":
                                await bot.close_position_async(task.alert.ticker)
                            else:
                                await bot.open_position_async(task.alert)
                            
                            duration = time.time() - start_time
                            wallet_display = f"{task.wallet_address[:10]}..." if task.wallet_address else "default"
                            info_logger.info(f"Worker {worker_id} completed {task.system_id}:{task.alert.ticker} for {wallet_display} in {duration:.2f}s")
                            
                        except Exception as e:
                            wallet_display = f"{task.wallet_address[:10]}..." if task.wallet_address else "default"
                            info_logger.error(f"Worker {worker_id} failed {task.system_id}:{task.alert.ticker} for {wallet_display}: {e}")
                        
                        finally:
                            self.task_queue.task_done()
                            # Semaphore released automatically when exiting 'async with' block
                            # This allows the next waiting worker to start processing
                            
                except asyncio.TimeoutError:
                    # No tasks available for 30 seconds, check if shutdown
                    continue
                except Exception as e:
                    info_logger.error(f"Worker {worker_id} error: {e}")
                    await asyncio.sleep(1)
                    
        finally:
            self._active_workers.discard(worker_id)
            info_logger.info(f"Worker {worker_id} shut down")
    

    # async def process_alerts_parallel(self, system_id: str, alerts: List[Alert]):
    #     """Process alerts in parallel with dynamic worker creation"""
    #     alert_count = len(alerts)
        
    #     info_logger.info(f"Processing {alert_count} alerts for {system_id}")
        
    #     # Calculate optimal worker count (don't spawn more workers than tasks)
    #     optimal_workers = min(alert_count, self.max_workers, 30)  # Cap at 30 for reasonable resource usage
        
    #     # Sort alerts by priority (exits first)
    #     sorted_alerts = sorted(alerts, key=lambda a: (TaskPriority.EXIT.value if a.action == "exit" else TaskPriority.OPEN.value))
        
    #     # Queue all tasks
    #     for alert in sorted_alerts:
    #         priority = TaskPriority.EXIT if alert.action == "exit" else TaskPriority.OPEN
    #         task = TradingTask(
    #             system_id=system_id,
    #             alert=alert,
    #             priority=priority,
    #             created_at=time.time()
    #         )
    #         await self.task_queue.put(task)
        
    #     # Create workers dynamically
    #     worker_tasks = []
    #     tasks_per_worker = max(1, alert_count // optimal_workers)
        
    #     for i in range(optimal_workers):
    #         worker_id = f"worker-{system_id}-{i}-{int(time.time())}"
    #         worker_task = asyncio.create_task(
    #             self._create_worker(worker_id, tasks_per_worker + (1 if i < alert_count % optimal_workers else 0))
    #         )
    #         worker_tasks.append(worker_task)
        
    #     # info_logger.info(f"Created {optimal_workers} workers for {alert_count} tasks ({system_id})")
        
    #     # Wait for all tasks to complete
    #     await self.task_queue.join()
        
    #     # Signal workers to shut down and wait for completion
    #     self._worker_shutdown_event.set()
    #     await asyncio.gather(*worker_tasks, return_exceptions=True)
    #     self._worker_shutdown_event.clear()
        
    #     info_logger.info(f"Completed processing {alert_count} alerts for {system_id}")


    async def process_alerts_for_users(self, system_id: str, alerts: List[Alert], users: List[Dict[str, str]]):
        """
        Process alerts for multiple users - parallel across users, sequential per user.
        
        This prevents nonce collisions by ensuring each wallet only processes one alert at a time,
        while still processing multiple users in parallel (up to max_concurrent_tasks).
        """
        total_tasks = len(alerts) * len(users)
        
        info_logger.info(f"Processing {len(alerts)} alerts for {len(users)} users ({total_tasks} total tasks)")
        info_logger.info(f"Rate limit: Max {self.max_concurrent_tasks} concurrent users")
        
        # Sort alerts by priority (exits first)
        sorted_alerts = sorted(alerts, key=lambda a: (TaskPriority.EXIT.value if a.action == "exit" else TaskPriority.OPEN.value))
        
        async def process_user_alerts(user: Dict[str, str]):
            """Process all alerts for a single user sequentially to avoid nonce collisions"""
            wallet_display = f"{user['walletAddress'][:10]}..."
            start_time = time.time()
            
            for alert in sorted_alerts:
                try:
                    bot = self.get_bot(system_id, api_key=user["apiKey"], wallet_address=user["walletAddress"])
                    
                    if alert.action == "exit":
                        await bot.close_position_async(alert.ticker)
                    else:
                        await bot.open_position_async(alert)
                    
                    info_logger.info(f"Completed {system_id}:{alert.ticker} for {wallet_display}")
                    
                except Exception as e:
                    info_logger.error(f"Failed {system_id}:{alert.ticker} for {wallet_display}: {e}")
            
            duration = time.time() - start_time
            info_logger.info(f"Finished all alerts for {wallet_display} in {duration:.2f}s")
        
        async def process_with_semaphore(user: Dict[str, str]):
            """Wrap user processing with semaphore to limit concurrent users"""
            async with self.worker_semaphore:
                await process_user_alerts(user)
        
        # Process all users in parallel (limited by semaphore), each user's alerts sequentially
        await asyncio.gather(*[process_with_semaphore(user) for user in users])
        
        info_logger.info(f"Completed processing {total_tasks} tasks for {system_id}")


# ///////////////////////////////////////////////////////////////////////////
# VALIDATION AND PROCESSING
# ///////////////////////////////////////////////////////////////////////////

def validate_message(message: str) -> tuple[str, AlertBatch]:
    """Validate message format: 'SYSTEM_ID [JSON_ARRAY]'"""
    # info_logger.info(f"Validating message: {message}")              ## remove

    parts = message.strip().split(maxsplit=1)
    if len(parts) != 2 or not parts[0].isalnum():
        raise ValueError('Invalid format: Expected "SYSTEM_ID [JSON_ARRAY]"')
    system_id = parts[0]
    
    


    if system_id not in SYSTEM_CONFIGS:
        info_logger.warning(f"Received alert for unknown system ID: {system_id}")
        raise ValueError(f'Invalid system ID: {system_id}')

    try:
        # Parse the JSON array
        alerts_array = json.loads(parts[1])
        # Create the AlertBatch with the parsed array
        batch_data = AlertBatch(alerts=alerts_array)
        return system_id, batch_data
    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON format: {e}')
    except ValueError as e:
        raise ValueError(f'Invalid alert batch: {e}')

# ///////////////////////////////////////////////////////////////////////////
# FASTAPI SETUP
# ///////////////////////////////////////////////////////////////////////////


def _load_tradingview_ips() -> Set[str]:
    """Load TradingView's official IP ranges"""
    # TradingView's official webhook IP ranges
    default_ips = [
        "52.89.214.238",
        "34.212.75.30", 
        "54.218.53.128",
        "52.32.178.7",
    ]
    return set(default_ips)

def verify_tradingview_ip(request: Request) -> bool:
    """Verify request comes from TradingView IPs"""
    
    # Get X-Forwarded-For header
    x_forwarded_for = request.headers.get("X-Forwarded-For")
    
    client_ip = None
    if x_forwarded_for:
        # X-Forwarded-For can be a comma-separated list, the first is the client's IP
        client_ip = x_forwarded_for.split(',')[0].strip()
    else:
        # Fallback to direct client host if X-Forwarded-For is not present
        # (e.g., during local development or if not behind a proxy)
        client_ip = request.client.host
    
    is_allowed = client_ip in _load_tradingview_ips()
    
    if not is_allowed:
        info_logger.warning(f"🚫 Blocked request from non-TradingView IP: {client_ip}")
    
    return is_allowed

def is_within_pre_hour_window() -> bool:
    """
    Check if current time is within 5 minutes before a new full hour.
    Returns True if current time is between XX:55:00 and XX:59:59
    """
    now = datetime.now(timezone.utc)
    current_minute = now.minute
    
    # Check if we're in the 5-minute window before the hour (minutes 55-59)
    return 55 <= current_minute <= 59

# Global parallel manager
parallel_bot_manager = ParallelBotManager(max_concurrent_tasks=7)

@app.middleware("http")
async def tradingview_security_middleware(request: Request, call_next):
    """TradingView-specific security middleware with minimal public endpoints"""
    path = request.url.path
    
    # Only allow browser/infrastructure paths without IP check
    if path in {"/favicon.ico", "/favicon.png", "/robots.txt"}:
        return await call_next(request)
    
    # Minimal root response (no endpoint processing)
    if path == "/":
        return JSONResponse({"status": "ok"})
    
            
    # Everything else requires TradingView IP verification
    if not verify_tradingview_ip(request):
        info_logger.warning(f"🚫 Blocked unauthorized access to {path} from {request.client.host}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied"
        )
    
    # Time-based filtering for webhook
    if path == "/CS-Automation" and request.method == "POST":
        if is_within_pre_hour_window():
            current_time = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            info_logger.info(f"🕐 Alert rejected due to pre-hour window filter at {current_time}")
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="False alert"
            )
    
    response = await call_next(request)
    return response  

# Updated webhook endpoint
@app.post("/CS-Automation")
async def receive_webhook_parallel(request: Request):
    """
    Parallel webhook endpoint with multi-user support.
    
    Flow:
    1. Validate incoming webhook message
    2. Load user credentials from environment variables
    3. Create trading tasks for each user
    4. Process trades in parallel
    """
    try:
        raw_message = await request.body()
        message = raw_message.decode('utf-8')
        
        info_logger.info(f"Received webhook: {message}")
        
        # Validate message format and parse alerts
        try:
            system_id, batch = validate_message(message)

        except ValueError as e:
            # Client error - invalid format or data
            info_logger.warning(f"Validation error: {e}")
            raise HTTPException(status_code=400, detail=str(e))
        except json.JSONDecodeError as e:
            # Client error - malformed JSON
            info_logger.warning(f"JSON decode error: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")
        except ValidationError as e:
            # Pydantic validation error - invalid field values
            info_logger.warning(f"Pydantic validation error: {e}")
            raise HTTPException(status_code=422, detail=str(e))
        
        # Load user credentials from environment variables
        users = []
        if WALLET1_ADDRESS and WALLET1_API_KEY:
            users.append({"walletAddress": WALLET1_ADDRESS, "apiKey": WALLET1_API_KEY})
        
        user_count = len(users)
        if user_count == 0:
            info_logger.warning("No wallet credentials configured. No trades will be executed.")
        
        info_logger.info(f"Found {user_count} user(s) from environment variables")
        
        # Process alerts in parallel for all users
        total_tasks = len(batch.alerts) * user_count
        info_logger.info(f"Creating {total_tasks} tasks ({len(batch.alerts)} alerts × {user_count} users)")
        
        try:
            await parallel_bot_manager.process_alerts_for_users(system_id, batch.alerts, users)
        except Exception as e:
            error_msg = f"Trade execution error: {e}"
            info_logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        
    except Exception as e:
        error_msg = f"Webhook error: {e}"
        info_logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)