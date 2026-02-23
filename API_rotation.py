


import os
import itertools
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# ------------------------------------------------------------------
# INTERNAL HELPER FUNCTIONS
# ------------------------------------------------------------------

def _log_key_usage(service_name: str, key: str, delay: float = 0):
    """
    Logs the currently used API key (masked for security)
    and optionally applies a delay before the next rotation.

    Args:
        service_name (str): API service name (e.g., GROQ, TAVILY, SERPAPI)
        key (str): Active API key
        delay (float): Optional delay (in seconds)
    """

    # Mask the key for security (only first 6 and last 4 characters shown)
    masked_key = key[:6] + "..." + key[-4:]
    timestamp = datetime.now().strftime("%H:%M:%S")

    print(f"🔁 [{timestamp}] {service_name} → Using API Key: {masked_key}")

    # Optional delay for rate-limit control
    if delay > 0:
        print(f"⏳ {service_name} → Waiting {delay} seconds before next rotation...")
        time.sleep(delay)


def _create_key_cycle(prefix: str):
    """
    Scans environment variables for API keys using this format:
    PREFIX, PREFIX_1, PREFIX_2, PREFIX_3, ...

    Returns:
        tuple: (itertools.cycle(keys), total_key_count)
    """

    keys = []

    # 1. Read base key (PREFIX)
    if os.getenv(prefix):
        keys.append(os.getenv(prefix))

    # 2. Read numbered keys (PREFIX_1, PREFIX_2, ...)
    i = 1
    while True:
        key = os.getenv(f"{prefix}_{i}")
        if key:
            keys.append(key)
            i += 1
        else:
            break

    # 3. Handle missing keys
    if not keys:
        print(f"⚠️  Warning: No API keys found for {prefix}")
        return None, 0

    print(f"✅ Key Manager: Loaded {len(keys)} keys for {prefix}")

    # Return infinite cycle iterator + total count
    return itertools.cycle(keys), len(keys)


# ------------------------------------------------------------------
# INITIALIZE ROTATORS
# ------------------------------------------------------------------

_groq_cycle, _groq_count     = _create_key_cycle("GROQ_API_KEY")
_tavily_cycle, _tavily_count = _create_key_cycle("TAVILY_API_KEY")
_serpapi_cycle, _serpapi_count = _create_key_cycle("SERPAPI_KEY")


# ------------------------------------------------------------------
# PUBLIC FUNCTIONS — GET API KEYS (WITH ROTATION + OPTIONAL DELAY)
# ------------------------------------------------------------------

def get_groq_key(delay: float = 0):
    """
    Returns the next Groq API key with optional delay.

    Args:
        delay (float): Optional delay in seconds
    """
    if _groq_cycle is None:
        raise ValueError("No GROQ API keys configured")

    key = next(_groq_cycle)
    _log_key_usage("GROQ", key, delay)
    return key


def get_tavily_key(delay: float = 0):
    """
    Returns the next Tavily API key with optional delay.

    Args:
        delay (float): Optional delay in seconds
    """
    if _tavily_cycle is None:
        raise ValueError("No TAVILY API keys configured")

    key = next(_tavily_cycle)
    _log_key_usage("TAVILY", key, delay)
    return key


def get_serpapi_key(delay: float = 0):
    """
    Returns the next SerpAPI key with optional delay.

    Args:
        delay (float): Optional delay in seconds
    """
    if _serpapi_cycle is None:
        raise ValueError("No SERPAPI keys configured")

    key = next(_serpapi_cycle)
    _log_key_usage("SERPAPI", key, delay)
    return key


# ------------------------------------------------------------------
# PUBLIC FUNCTIONS — GET TOTAL KEY COUNTS
# ------------------------------------------------------------------

def get_groq_count():
    """Returns total number of loaded Groq API keys."""
    return _groq_count


def get_tavily_count():
    """Returns total number of loaded Tavily API keys."""
    return _tavily_count


def get_serpapi_count():
    """Returns total number of loaded SerpAPI keys."""
    return _serpapi_count
