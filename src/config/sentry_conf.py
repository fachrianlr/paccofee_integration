import os

import sentry_sdk
from dotenv import load_dotenv
from sentry_sdk.integrations.logging import LoggingIntegration

from src.config.logging_conf import logger, logging

# Get SENTRY_DSN from environment variables
load_dotenv()
SENTRY_DSN = os.environ.get("SENTRY_DSN")

if SENTRY_DSN:
    sentry_logging = LoggingIntegration(event_level=logging.ERROR)

    sentry_sdk.init(
        dsn=SENTRY_DSN,
        integrations=[sentry_logging],
        traces_sample_rate=1.0,
        send_default_pii=True,
    )

    SENTRY_LOGGING = sentry_sdk
else:
    logger.error("SENTRY_DSN environment variable not set. Sentry SDK not initialized.")
    SENTRY_LOGGING = None
