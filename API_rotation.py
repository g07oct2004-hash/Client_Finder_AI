# import os
# import itertools
# from dotenv import load_dotenv

# # Load environment variables once upon import
# load_dotenv()

# # --- INTERNAL HELPER (NOT FOR USE IN OTHER FILES) ---
# def _create_key_cycle(prefix):
#     """
#     Finds keys (PREFIX, PREFIX_1, PREFIX_2...) and creates an infinite rotator.
#     """
#     keys = []
    
#     # 1. Check for the base key (e.g., GROQ_API_KEY)
#     if os.getenv(prefix):
#         keys.append(os.getenv(prefix))

#     # 2. Check for numbered keys (e.g., GROQ_API_KEY_1, GROQ_API_KEY_2...)
#     i = 1
#     while True:
#         key = os.getenv(f"{prefix}_{i}")
#         if key:
#             keys.append(key)
#             i += 1
#         else:
#             break
            
#     # 3. Handle Empty Case (No keys found)
#     if not keys:
#         return None  # Return None so we can raise a specific error later

#     # 4. Handle Success (1 or more keys)
#     # itertools.cycle(['A']) -> 'A', 'A', 'A'... (Works for 1 key)
#     # itertools.cycle(['A', 'B']) -> 'A', 'B', 'A'... (Works for multiple)
#     print(f" Key Manager: Loaded {len(keys)} keys for {prefix}")
#     return itertools.cycle(keys)

# # --- INITIALIZE ROTATORS (Module Level State) ---
# _groq_cycle = _create_key_cycle("GROQ_API_KEY")
# _tavily_cycle = _create_key_cycle("TAVILY_API_KEY")
# _serpapi_cycle = _create_key_cycle("SERPAPI_KEY")
# _rapidapi_cycle = _create_key_cycle("RAPIDAPI_KEY")

# # --- PUBLIC FUNCTIONS (IMPORT THESE) ---

# def get_groq_key():
#     """Returns the next GROQ key or raises error if none exist."""
#     if _groq_cycle is None:
#         raise ValueError(" CRITICAL: No 'GROQ_API_KEY' found in .env file!")
#     return next(_groq_cycle)

# def get_tavily_key():
#     """Returns the next TAVILY key or raises error if none exist."""
#     if _tavily_cycle is None:
#         raise ValueError(" CRITICAL: No 'TAVILY_API_KEY' found in .env file!")
#     return next(_tavily_cycle)

# def get_serpapi_key():
#     """Returns the next SERPAPI key or raises error if none exist."""
#     if _serpapi_cycle is None:
#         raise ValueError(" CRITICAL: No 'SERPAPI_KEY' found in .env file!")
#     return next(_serpapi_cycle)

# def get_rapidapi_key():
#     """Returns the next RAPIDAPI key or raises error if none exist."""
#     if _rapidapi_cycle is None:
#         raise ValueError(" CRITICAL: No 'RAPIDAPI_KEY' found in .env file!")
#     return next(_rapidapi_cycle)



import os
import itertools
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- INTERNAL HELPER ---
def _create_key_cycle(prefix):
    """
    Scans .env for keys (PREFIX, PREFIX_1, PREFIX_2...).
    Returns a tuple: (cycle_iterator, total_count)
    """
    keys = []
    
    # 1. Check for base key
    if os.getenv(prefix):
        keys.append(os.getenv(prefix))

    # 2. Check for numbered keys
    i = 1
    while True:
        key = os.getenv(f"{prefix}_{i}")
        if key:
            keys.append(key)
            i += 1
        else:
            break
            
    # 3. Handle Empty Case
    if not keys:
        print(f" Warning: No keys found for {prefix}")
        return None, 0

    print(f"✅ Key Manager: Loaded {len(keys)} keys for {prefix}")
    
    # RETURN BOTH: The Infinite Cycle AND The Total Count
    return itertools.cycle(keys), len(keys)

# --- INITIALIZE ROTATORS & COUNTS ---
# We unpack the tuple here: (cycle, count) = function()

_groq_cycle, _groq_count = _create_key_cycle("GROQ_API_KEY")
_tavily_cycle, _tavily_count = _create_key_cycle("TAVILY_API_KEY")
_serpapi_cycle, _serpapi_count = _create_key_cycle("SERPAPI_KEY")
# _rapidapi_cycle, _rapidapi_count = _create_key_cycle("RAPIDAPI_KEY")
# _google_cycle, _google_count = _create_key_cycle("GOOGLE_API_KEY")

# --- PUBLIC FUNCTIONS (Get Keys) ---

def get_groq_key():
    if _groq_cycle is None: raise ValueError("No GROQ keys found")
    return next(_groq_cycle)

def get_tavily_key():
    if _tavily_cycle is None: raise ValueError("No TAVILY keys found")
    return next(_tavily_cycle)

def get_serpapi_key():
    if _serpapi_cycle is None: raise ValueError("No SERPAPI keys found")
    return next(_serpapi_cycle)


# def get_rapidapi_key():
#     """Returns the next RAPIDAPI key or raises error if none exist."""
#     if _rapidapi_cycle is None:
#         raise ValueError(" CRITICAL: No 'RAPIDAPI_KEY' found in .env file!")
#     return next(_rapidapi_cycle)


# --- PUBLIC FUNCTIONS (Get Counts) ---
# Use these to make your loops dynamic!

def get_groq_count():
    return _groq_count

def get_tavily_count():
    return _tavily_count

def get_serpapi_count():
    return _serpapi_count