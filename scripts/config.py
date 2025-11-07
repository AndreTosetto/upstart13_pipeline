"""
Pipeline configuration set by CLI flags.

This module stores runtime configuration that can be overridden via run_pipeline.py flags.
"""
import os

# Default configuration
PRICE_MODE = os.environ.get('UPSTART_PRICE_MODE', 'spec')  # 'spec' or 'correct'
IMPUTE_ORDERDATE = os.environ.get('UPSTART_IMPUTE_ORDERDATE', 'false').lower() == 'true'

def set_config(price_mode='spec', impute_orderdate=False):
    """Set configuration from command-line arguments."""
    global PRICE_MODE, IMPUTE_ORDERDATE
    PRICE_MODE = price_mode
    IMPUTE_ORDERDATE = impute_orderdate
