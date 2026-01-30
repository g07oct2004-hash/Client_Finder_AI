


import os
import time
import json
import pandas as pd
import re
import asyncio
from dotenv import load_dotenv
from groq import AsyncGroq
import gspread
from google.oauth2.service_account import Credentials
import itertools

# --- 1. SETUP & CONFIGURATION ---
load_dotenv()

def get_api_keys(prefix):
    """Fetches all available API keys from .env."""
    keys = []
    i = 1
    while True:
        key = os.getenv(f"{prefix}_{i}")
        if key: keys.append(key); i += 1
        else: break
    if not keys and os.getenv(prefix): keys.append(os.getenv(prefix))
    return keys

GROQ_KEYS = get_api_keys("GROQ_API_KEY")
KEY_CYCLE = itertools.cycle(GROQ_KEYS) if GROQ_KEYS else None



def connect_to_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    return gspread.authorize(creds)

# --- 2. SURGICAL DATA CLEANING ---

def clean_and_compress_text(raw_text):
    """
    Aggressive Cleaner: Removes '...' garbage, metadata tags, and broken words.
    """
    if not raw_text or not isinstance(raw_text, str):
        return ""

    # --- PHASE 1: GARBAGE REMOVAL ---
    text = re.sub(r'\|\s*\.\.\.\s*\|', ' ', raw_text)
    text = re.sub(r'\.\.\.', ' ', text)
    text = re.sub(r'\b(name:|role:|url:|related_companies:|related_people:|image \d+:)', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'\b\w{1,5}\.\.\.\b', ' ', text) 

    noise_phrases = [
        "View All Customers", "Contact Us", "Skip to main content", "Agree & Join LinkedIn",
        "Sign in", "Join now", "Report this post", "See more comments", "Follow us",
        "Copyright", "Cookie Policy", "User Agreement", "About Us", "Log In", "Subscribe",
        "Show more", "Show less", "View Profile", "Forgot password", "Share", "Comment", "Like"
    ]
    pattern = r'\b(?:' + '|'.join(map(re.escape, noise_phrases)) + r')\b'
    text = re.sub(pattern, ' ', text, flags=re.IGNORECASE)

    text = re.sub(r"['\[\]]", "", text) 
    text = re.sub(r"\\u[0-9a-fA-F]{4}", "", text) 
    text = re.sub(r"\s+", " ", text).strip() 

    # --- PHASE 2: THE FILTER ---
    chunks = re.split(r'[|;]', text)
    
    money_pattern = r'(\$|€|£|¥|₹|Rs\.|INR|USD|EUR|GBP)\s?\d+(?:[.,]\d+)?\s?(?:k|m|b|t|million|billion|crore|lakh)?'
    
    safe_keywords = [
        'funding', 'raised', 'series', 'seed', 'venture', 'capital', 'equity', 
        'investor', 'backed', 'debt', 'financing', 'angel', 'pre-seed',
        'ipo', 'nasdaq', 'nyse', 'ticker', 'stock', 'share', 'dividend', 
        'market cap', 'publicly traded', 'listed', 'annual revenue', 'earnings', 
        'acquisition', 'acquired', 'merged', 'unicorn', 'growth partners',
        'investment', 'private equity', 'valuation'
    ]
    
    unique_kept_chunks = [] 
    seen_hashes = set()

    for chunk in chunks:
        chunk = chunk.strip()
        if len(chunk) < 15: continue 
        
        chunk_hash = hash(chunk.lower())
        if chunk_hash in seen_hashes: continue
        
        chunk_lower = chunk.lower()
        has_money = re.search(money_pattern, chunk, re.IGNORECASE)
        has_keyword = any(k in chunk_lower for k in safe_keywords)
        
        if has_money or has_keyword:
            unique_kept_chunks.append(chunk)
            seen_hashes.add(chunk_hash)

    final_text = " | ".join(unique_kept_chunks)
    return final_text[:3000]

def get_funding_related_snippets(row):
    full_text_blob = ""
    target_cols = [
        'company_profile_industry', 'company_profile_tagline',
        'leadership_team_founders', 'leadership_team_board_members',
        'leadership_team_key_people'
    ]
    
    for col in row.index:
        if "news" in col.lower() or col in target_cols:
            val = str(row[col])
            if len(val) > 2: full_text_blob += " " + val

    return clean_and_compress_text(full_text_blob)

# --- 3. FILTER & VALIDATION LOGIC ---

def is_significant_amount(text):
    text_lower = text.lower()
    magnitude_words = ['million', 'billion', 'trillion', 'crore', 'lakh', 'mn', 'bn', 'tn', 'cr', 'k', 'm', 'b']
    if any(word in text_lower for word in magnitude_words): return True
    if "," in text and any(c.isdigit() for c in text): return True
    return False

def clean_and_validate_output(text):
    clean_text = text.replace('"', '').replace("'", "").strip()
    if "public" in clean_text.lower(): return "Public Listed"
    if "," in clean_text: clean_text = clean_text.split(",")[0].strip()
    if "(" in clean_text: clean_text = clean_text.split("(")[0].strip()
    
    if len(clean_text) > 60: return "Bootstrapped"
    
    has_currency = any(s in clean_text.lower() for s in ['$', '£', '€', '₹', 'rs'])
    has_digit = any(c.isdigit() for c in clean_text)
    
    if not (has_currency and has_digit): return "Bootstrapped"
    if not is_significant_amount(clean_text): return "Bootstrapped"
    return clean_text

# --- 4. ASYNC AI BRAIN ---


async def analyze_funding_async(company_name, text_data, sem):
    """
    Async function to call Groq.
    """
    if not text_data or len(text_data) < 10:
        return "Bootstrapped"

    if not KEY_CYCLE:
        return "Error"

    prompt = f"""
    ### TASK
    You are a Financial Data Analyst. Analyze the snippets below to determine the company's FUNDING STATUS.
    
    ### DATA
    {text_data}

    ### RULES
    1. Output ONLY the Amount (e.g., "$15 Million", "₹50 Crore") OR "Public Listed" OR "Bootstrapped".
    2. **STRICT PUBLIC LISTED CHECK (CRITICAL):** - ONLY output "Public Listed" if the text explicitly mentions: "NASDAQ", "NYSE", "Stock Symbol", "Ticker:", "IPO", or "Publicly Traded".
       - **DO NOT** output "Public Listed" just because you see words like "Investment", "Venture", "Finance", "Share Capital", or "Global". These are common for private companies too.
    3. If text mentions "Raised", "Secured", "Series A/B": Output the Amount.
    4. If no external funding is found, Output "Bootstrapped".
    """

   
    async with sem:
        # Retry Logic with Key Rotation
        for _ in range(len(GROQ_KEYS)):
            try:
                current_key = next(KEY_CYCLE)
                client = AsyncGroq(api_key=current_key)
                
                response = await client.chat.completions.create(
                    messages=[{"role": "user", "content": prompt}],
                    model="llama-3.3-70b-versatile",
                    temperature=0.0
                )
                return response.choices[0].message.content.strip()
            
            except Exception as e:
                print(f" ⚠️ Key Error for {company_name}: {e}")
                if "429" in str(e):
                    await asyncio.sleep(2) 
        
        return "Error"

# --- 5. MAIN ASYNC ORCHESTRATOR ---

async def process_row_task(index, row, worksheet, col_map, sem):
    company_name = str(row.get('company_profile_company_name', f"Row_{index+1}"))
    
    # 1. Skip Logic
    current_val = str(row.get('Funding Status', '')).strip()
    if current_val and "unclear" not in current_val.lower() and "error" not in current_val.lower():
        print(f" Skipping {company_name} (Already Done)")
        return

    # 2. Extract & Clean Data
    filtered_data = get_funding_related_snippets(row)
    
    if filtered_data:
        print(f" Analyzing {company_name} | Sending {len(filtered_data)} chars to AI")
    else:
        print(f" {company_name}: Cleaned data is empty (Auto-Bootstrapped)")
        filtered_data = ""

    # 3. AI Analysis
   
    raw_status = await analyze_funding_async(company_name, filtered_data, sem)
    
    final_status = "Error"
    if raw_status != "Error":
        final_status = clean_and_validate_output(raw_status)

    # 4. Update Sheet
    try:
        worksheet.update_cell(index + 2, col_map['Funding Status'], final_status)
        print(f" {company_name}: {final_status}")
            
    except Exception as e:
        print(f" Write Error {company_name}: {e}")

async def main_async():
    print(" Connecting to Sheets...")
    

    sem = asyncio.Semaphore(2)

    gc = connect_to_sheet()
    sh = gc.open(os.getenv("GOOGLE_SHEET_NAME", "Company_data"))
    worksheet = sh.sheet1
    
    raw_data = worksheet.get_all_values()
    if not raw_data: return

    headers = raw_data[0]
    df = pd.DataFrame(raw_data[1:], columns=headers)
    
    if "Funding Status" not in df.columns:
        print(" Adding 'Funding Status' column...")
        worksheet.add_cols(1)
        worksheet.update_cell(1, len(headers) + 1, "Funding Status")
        headers.append("Funding Status")
        df = pd.DataFrame(raw_data[1:], columns=headers) 

    col_map = {name: i + 1 for i, name in enumerate(headers)}

    tasks = []
    print(f" Starting Async Analysis for {len(df)} companies...")
    
    for index, row in df.iterrows():
    
        tasks.append(process_row_task(index, row, worksheet, col_map, sem))
    
    await asyncio.gather(*tasks)
    print(" All Funding Analysis Completed.")

if __name__ == "__main__":
    asyncio.run(main_async())