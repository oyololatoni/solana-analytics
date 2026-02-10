import logging
import sys

# Configure structured logging for the API package
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
    stream=sys.stdout
)

logger = logging.getLogger("solana-analytics")
