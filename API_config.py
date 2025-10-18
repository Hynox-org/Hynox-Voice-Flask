import os
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file

# API Key Config
GEMINI_API_KEYS = [os.getenv(f"GEMINI_API_KEY_{i}") for i in range(1, 6) if os.getenv(f"GEMINI_API_KEY_{i}")]
current_key_index = 0

def get_next_gemini_api_key():
    global current_key_index
    if not GEMINI_API_KEYS:
        return None
    key = GEMINI_API_KEYS[current_key_index]
    current_key_index = (current_key_index + 1) % len(GEMINI_API_KEYS)
    return key

GEMINI_API_KEY = get_next_gemini_api_key() # For backward compatibility if only one key is expected
