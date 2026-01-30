# import json
# import time
# import random
# import requests
# import os
# import datetime
# from bs4 import BeautifulSoup
# from groq import Groq
# from fake_useragent import UserAgent
 
# # ==========================================
# # 🟢 1. CONFIGURATION
# # ==========================================
 
# TARGET_COMPANIES = [
    
# ]
 
# FINAL_OUTPUT_FILE = "company_intel/Final_Company_Data_by_simple_approach.json"
# RAW_DEBUG_FILE = "raw_search_logs_by_simple_approach.txt"
# # Ensure output directory exists
# os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

 
# # # 🔑 API Key (Replace with yours if not in environment)
# # GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
 
# # # Initialize AI Client
# # client = Groq(api_key=GROQ_API_KEY)

# # --- 🔑 DYNAMIC KEY LOADER ---
# def get_api_keys(prefix):
#     keys = []
#     i = 1
#     while True:
#         key = os.environ.get(f"{prefix}_{i}")
#         if key:
#             keys.append(key)
#             i += 1
#         else:
#             break
#     # Fallback: if without Number API key
#     if not keys and os.environ.get(prefix):
#         keys.append(os.environ.get(prefix))
#     return keys

# # Load all Groq Keys
# GROQ_KEYS = get_api_keys("GROQ_API_KEY")


 
# # ==========================================
# # 🟢 2. HELPER FUNCTIONS
# # ==========================================
 
# def save_raw_log(company, query, raw_text):
#     """Saves the raw text to a file so you can check it later."""
#     with open(RAW_DEBUG_FILE, "a", encoding="utf-8") as f:
#         f.write(f"\n{'='*50}\n🏢 {company} | 🔍 {query}\n{'-'*20}\n{raw_text}\n{'='*50}\n")
 
# def save_json(data):
#     """Saves the clean data to the JSON file."""
#     with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
#         json.dump(data, f, indent=4, ensure_ascii=False)
 
# # ==========================================
# # 🟢 3. SEARCH ENGINE (DuckDuckGo)
# # ==========================================
 
# def search_ddg(query, time_filter=None):
#     """
#     Searches DuckDuckGo.
#     - query: The text to search.
#     - time_filter: 'y' to get only results from the Past Year.
#     """
#     url = "https://html.duckduckgo.com/html/"
#     ua = UserAgent()
   
#     # Headers make us look like a real browser coming from Google
#     headers = {
#         "User-Agent": ua.random,
#         "Referer": "https://www.google.com/",
#         "Accept-Language": "en-US,en;q=0.9"
#     }
   
#     payload = {"q": query}
#     if time_filter:
#         payload["df"] = time_filter  # This activates "Past Year" filter
       
#     try:
#         print(f"      📡 Searching: '{query}'...")
#         response = requests.post(url, data=payload, headers=headers, timeout=20)
       
#         if response.status_code == 200:
#             # Check for Blocks
#             if "captcha" in response.text.lower() or "too many requests" in response.text.lower():
#                 return "BLOCK"
 
#             soup = BeautifulSoup(response.text, "html.parser")
#             results = soup.find_all("div", class_="result__body", limit=10) # Get top 10 results
           
#             combined_text = ""
#             for res in results:
#                 title = res.find("a", class_="result__a").get_text(strip=True)
#                 snippet = res.find("a", class_="result__snippet").get_text(strip=True)
#                 combined_text += f"Source: {title}\nSnippet: {snippet}\n{'-'*10}\n"
           
#             return combined_text if combined_text.strip() else None
 
#         elif response.status_code in [429, 403]:
#             return "BLOCK"
           
#         return None
 
#     except Exception as e:
#         print(f"      ⚠️ Network Error: {e}")
#         return None
 
# # ==========================================
# # 🟢 4. AI ANALYST (Groq)
# # ==========================================
# def analyze_with_groq(company_name, raw_data):
#     """
#     Sends the gathered data to Groq to extract the single best answer.
#     Uses API Key Rotation to ensure reliability.
#     """
#     # Define the system prompt with strict decision-making rules
#     system_prompt = (
#         "You are a Senior Financial Data Analyst. Your job is to determine the single most accurate "
#         "Revenue and Employee count for a company based on search snippets.\n\n"
        
#         "RULES FOR DECISION MAKING:\n"
#         "1. **Revenue:**\n"
#         "   - PRIORITY 1: If you see an INR (₹) figure from official sources (Tracxn, Zaubacorp, News) for FY24/25, USE IT. Convert to a clean string (e.g., '₹275 Cr').\n"
#         "   - PRIORITY 2: If no INR figure exists, use the most credible USD figure (e.g., from RocketReach or Press Release). \n"
#         "   - IGNORE: 'Growjo' or 'ZoomInfo' if they look like automated estimates (e.g., revenue per employee calculations).\n"
#         "2. **Employees:**\n"
#         "   - Trust 'RocketReach' or 'LinkedIn' snippets the most.\n"
#         "   - Prefer exact numbers (e.g., 402) over ranges (e.g., 200-500).\n"
#         "3. **Output Format:**\n"
#         "   - Return ONLY a simple JSON object. No lists, no sources, no explanations.\n"
#         "   - Keys must be exactly: 'Annual Revenue' and 'Total Employee Count'.\n\n"
#         "FORMAT EXAMPLE:\n"
#         "{\n"
#         '  "Annual Revenue": "$3 million",\n'
#         '  "Total Employee Count": 31\n'
#         "}"
#     )
    
#     user_content = f"Target Company: {company_name}\n\nSearch Snippets:\n{raw_data}"

#     # 🔄 ROTATION LOGIC: Iterate through all available API keys
#     for index, api_key in enumerate(GROQ_KEYS):
#         try:
#             # Initialize the client with the current key in the loop
#             client = Groq(api_key=api_key)
            
#             completion = client.chat.completions.create(
#                 model="llama-3.3-70b-versatile",
#                 messages=[
#                     {"role": "system", "content": system_prompt},
#                     {"role": "user", "content": user_content}
#                 ],
#                 temperature=0,
#                 response_format={"type": "json_object"}
#             )
#             return json.loads(completion.choices[0].message.content)

#         except Exception as e:
#             print(f"      ⚠️ Groq Key {index+1} Failed: {e}")
            
#             # Check if there are more keys available to try
#             if index < len(GROQ_KEYS) - 1:
#                 print("      🔄 Switching to next API Key...")
#                 time.sleep(5) # Short pause before the next attempt
#             else:
#                 # All keys have failed
#                 print("      ❌ All Groq API Keys failed.")
#                 return {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}

# # ==========================================
# # 🟢 5. MAIN LOGIC LOOP
# # ==========================================
 
# def main():
#     # 1. Load existing data (Resume capability)
#     final_data = {}
#     if os.path.exists(FINAL_OUTPUT_FILE):
#         try:
#             with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
#                 final_data = json.load(f)
#         except:
#             pass
 
#     # 2. Calculate dynamic previous year (e.g., 2025)
#     current_year = datetime.datetime.now().year
#     prev_year = current_year - 1
 
#     print(f"🚀 Starting Extraction for {len(TARGET_COMPANIES)} Companies...")
#     print(f"📅 Target Revenue Year: {prev_year}\n")
 
#     for i, company in enumerate(TARGET_COMPANIES):
       
#         # Skip if already done
#         if company in final_data:
#             print(f"⏭️  Skipping {company} (Already Done)")
#             continue
 
#         print(f"[{i+1}/{len(TARGET_COMPANIES)}] 🏢 Processing: {company}")
       
#         # 🟢 DEFINING THE 2 SPECIFIC QUERIES
#         # Q1: Employee focus (RocketReach)
#         # Q2: Revenue focus (Annual + Year)
       
#         queries = [
#             # 1. Employee Query (RocketReach)
#             {
#                 "text": f"site:rocketreach.co {{{company}}} employees size",
#                 "filter": None
#             },
#             # 2. Revenue Query (Time Filtered)
#             {
#                 "text": f"{{{company}}} revenue annual {prev_year}",
#                 "filter": "y" # Past Year Filter
#             }
#         ]
 
#         full_raw_data = ""
 
#         # 🟢 RUNNING BOTH QUERIES ONE BY ONE
#         for q in queries:
#             query_text = q["text"]
#             time_filter = q["filter"]
           
#             snippet_text = None
           
#             # Retry Loop (If blocked)
#             for attempt in range(3):
#                 snippet_text = search_ddg(query_text, time_filter)
               
#                 if snippet_text == "BLOCK":
#                     wait = random.uniform(30, 60)
#                     print(f"      🛑 Blocked! Sleeping {wait:.1f}s...")
#                     time.sleep(wait)
#                     continue
               
#                 if snippet_text: break
#                 time.sleep(2) # Short wait between retries
           
#             if snippet_text:
#                 # Add result to our data pile
#                 full_raw_data += f"\nQUERY: {query_text}\n{snippet_text}\n"
#                 save_raw_log(company, query_text, snippet_text)
#             else:
#                 print(f"      🔸 No data found for query.")
 
#             # ⏳ DELAY BETWEEN QUERIES (Safe Time)
#             delay = random.uniform(5, 8)
#             print(f"      ⏳ Waiting {delay:.1f}s...")
#             time.sleep(delay)
 
#         # 🟢 FINAL ANALYSIS
#         if full_raw_data.strip():
#             print(f"      🧠 Analyzing with Groq...")
#             result = analyze_with_groq(company, full_raw_data)
#             print(f"      ✅ RESULT: {json.dumps(result)}")
           
#             # Save Data
#             final_data[company] = result
#             save_json(final_data)
#         else:
#             print(f"      ❌ NO DATA extracted.")
#             final_data[company] = {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}
#             save_json(final_data)
 
#         cooldown = random.uniform(10, 15)
#         print(f"[SLEEP] Cooling down for {cooldown:.1f}s before next company...\n")
#         time.sleep(cooldown)
 
#     print("\n🎉 All Done! Check Final_Company_Data.json")

# def enrich_companies_from_list(company_list):
#     global TARGET_COMPANIES
#     TARGET_COMPANIES = list(set(company_list))
#     main()

#--------------------------------------------------------------------------------

# import json
# import time
# import random
# import requests
# import os
# import datetime
# from bs4 import BeautifulSoup
# from groq import Groq
# from fake_useragent import UserAgent
# from duckduckgo_search import DDGS
# from dotenv import load_dotenv  

# # Load environment variables
# load_dotenv()
# # ==========================================
# # 🟢 1. CONFIGURATION (CONTROL CENTER)
# # ==========================================

# TARGET_COMPANIES = [
#     "AnavClouds Software Solutions",
#     "Fractal analytics",
#     "Metacube",
#     "AnavClouds analytics.ai",
#     "Cyntexa"

# ]

# FINAL_OUTPUT_FILE = "company_intel/Final_Company_Data_by_simple_approach.json"
# RAW_DEBUG_FILE = "raw_search_logs_by_simple_approach.txt"
# # Ensure output directory exists
# os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

# # ------------------------------------------
# # ⏱️ TIMING & DELAY SETTINGS
# # ------------------------------------------
# HTML_MAX_RETRIES = 3         
# LIBRARY_MAX_RETRIES = 3      
# SAFETY_MODE_LIMIT = 10       

# BLOCK_RETRY_DELAY = (20, 40)     ## After Blocking delay 
# QUERY_GAP_DELAY        = (5, 8)    # each query between delay's
# SAFETY_MODE_DELAY      = (15, 25) ## For duckduckgo_search Library 
# COMPANY_COOLDOWN_DELAY = (10, 20)  ## for each Company 

# # ------------------------------------------
# # 🔑 API KEYS
# # ------------------------------------------
# def get_api_keys(prefix):
#     keys = []
#     i = 1
#     while True:
#         key = os.environ.get(f"{prefix}_{i}")
#         if key: keys.append(key); i += 1
#         else: break
#     if not keys and os.environ.get(prefix): keys.append(os.environ.get(prefix))
#     return keys

# GROQ_KEYS = get_api_keys("GROQ_API_KEY")

# # ==========================================
# # 🟢 2. SAFE SAVING FUNCTIONS (UPDATED)
# # ==========================================

# def save_raw_log(company, query, raw_text):
#     """Saves raw logs safely without crashing."""
#     try:
#         with open(RAW_DEBUG_FILE, "a", encoding="utf-8") as f:
#             f.write(f"\n{'='*50}\n🏢 {company} | 🔍 {query}\n{'-'*20}\n{raw_text}\n{'='*50}\n")
#     except Exception as e:
#         print(f"      ⚠️ Error Saving Raw Log: {e}")

# def save_json(data):
#     """Saves JSON safely without crashing."""
#     try:
#         with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
#             json.dump(data, f, indent=4, ensure_ascii=False)
#     except Exception as e:
#         print(f"      ⚠️ Error Saving JSON: {e}")

# # ==========================================
# # 🟢 3. SEARCH ENGINES
# # ==========================================

# def search_via_html(query, time_filter=None):
#     url = "https://html.duckduckgo.com/html/"
#     ua = UserAgent()
#     headers = {"User-Agent": ua.random, "Referer": "https://www.google.com/"}
#     payload = {"q": query}
#     if time_filter: payload["df"] = time_filter
        
#     try:
#         print(f"      📡 [HTML] Searching: '{query}'...")
#         response = requests.post(url, data=payload, headers=headers, timeout=15)
        
#         if response.status_code in [429, 403] or "captcha" in response.text.lower():
#             return "BLOCK"

#         soup = BeautifulSoup(response.text, "html.parser")
#         results = soup.find_all("div", class_="result__body", limit=10)
        
#         if not results: return None 
        
#         combined_text = ""
#         for res in results:
#             title = res.find("a", class_="result__a").get_text(strip=True)
#             snippet = res.find("a", class_="result__snippet").get_text(strip=True)
#             combined_text += f"Source: {title}\nSnippet: {snippet}\n{'-'*10}\n"
#         return combined_text if combined_text.strip() else None

#     except Exception:
#         return "BLOCK"

# def search_via_library(query, time_filter=None):
#     try:
#         timelimit = "y" if time_filter == "y" else None
#         print(f"      🛡️ [LIBRARY] Searching: '{query}'...")
        
#         with DDGS() as ddgs:
#             results = list(ddgs.text(query, max_results=10, timelimit=timelimit))
#             if not results: return None
            
#             combined_text = ""
#             for res in results:
#                 combined_text += f"Source: {res.get('title')}\nSnippet: {res.get('body')}\n{'-'*10}\n"
#             return combined_text
            
#     except Exception as e:
#         print(f"      ⚠️ Library Exception: {e}")
#         return "BLOCK"



# def analyze_with_groq(company, raw_data):
#     # 🛑 Safety Check 1: Agar keys ki list hi khali hai
#     if not GROQ_KEYS:
#         print("      ❌ No GROQ Keys found!")
#         return {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}

#     system_prompt = (
#         "Extract 'Annual Revenue' and 'Total Employee Count'. "
#         "Return JSON only: {'Annual Revenue': '...', 'Total Employee Count': ...}."
#     )
#     user_content = f"Company: {company}\nData:\n{raw_data}"

#     for i, key in enumerate(GROQ_KEYS):
#         try:
#             client = Groq(api_key=key)
#             completion = client.chat.completions.create(
#                 model="llama-3.3-70b-versatile",
#                 messages=[{"role":"system","content":system_prompt},{"role":"user","content":user_content}],
#                 response_format={"type": "json_object"}
#             )
#             return json.loads(completion.choices[0].message.content)
#         except Exception as e:
#             print(f"      ⚠️ Key {i+1} Error: {e}")
            
#             # Agar ye last key thi aur wo bhi fail ho gayi
#             if i == len(GROQ_KEYS) - 1: 
#                 return {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}
            
#             # Agar aur keys bachi hain to wait karo aur next try karo
#             time.sleep(1)

#     # 🛑 Safety Check 2: Agar loop bina result ke khatam ho jaye (Null Protection)
#     return {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}
# # ==========================================
# # 🟢 5. MAIN LOGIC
# # ==========================================

# def main():
#     final_data = {}
    
#     # 🟢 Safe Load Logic (Corrupt file handle karega)
#     if os.path.exists(FINAL_OUTPUT_FILE):
#         try:
#             with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
#                 content = f.read().strip()
#                 if content:
#                     final_data = json.loads(content)
#         except Exception as e:
#             print(f"⚠️ Warning: Could not load previous data ({e}). Starting fresh.")
#             final_data = {}

#     prev_year = datetime.datetime.now().year - 1
#     safety_mode_steps_remaining = 0 

#     print(f"🚀 Starting Extraction for {len(TARGET_COMPANIES)} Companies...")

#     for i, company in enumerate(TARGET_COMPANIES):
#         if company in final_data:
#             print(f"⏭️  Skipping {company}")
#             continue

#         if safety_mode_steps_remaining > 0:
#             current_mode = "LIBRARY_MODE"
#             safety_mode_steps_remaining -= 1
#             print(f"\n🐢 [SAFETY MODE] Library Active. Remaining: {safety_mode_steps_remaining}")
#         else:
#             current_mode = "HTML_MODE"
#             print(f"\n⚡ [NORMAL MODE] HTML Active.")

#         print(f"[{i+1}/{len(TARGET_COMPANIES)}] 🏢 Processing: {company}")
        
#         queries = [
#             {"text": f"site:rocketreach.co {company} employees size", "filter": None},
#             {"text": f"{company} revenue annual {prev_year}", "filter": "y"}
#         ]
#         full_raw_data = ""

#         for q in queries:
#             result = None
            
#             # --- PATH 1: HTML MODE ---
#             if current_mode == "HTML_MODE":
#                 for attempt in range(HTML_MAX_RETRIES):
#                     result = search_via_html(q["text"], q["filter"])
#                     if result != "BLOCK": break 
                    
#                     wait = random.uniform(*BLOCK_RETRY_DELAY)
#                     print(f"      🛑 HTML Blocked (Attempt {attempt+1}). Sleeping {wait:.1f}s...")
#                     time.sleep(wait)
                
#                 if result == "BLOCK":
#                     print("      🚨 HTML FAILED 3 TIMES! Switching to Library.")
#                     safety_mode_steps_remaining = SAFETY_MODE_LIMIT
#                     current_mode = "LIBRARY_MODE" 
#                     result = None 

#             # --- PATH 2: LIBRARY MODE ---
#             if current_mode == "LIBRARY_MODE":
#                 for attempt in range(LIBRARY_MAX_RETRIES):
#                     result = search_via_library(q["text"], q["filter"])
#                     if result != "BLOCK": break 
                    
#                     wait = random.uniform(*BLOCK_RETRY_DELAY)
#                     print(f"      🛑 Library Blocked (Attempt {attempt+1}). Sleeping {wait:.1f}s...")
#                     time.sleep(wait)

#             if result and result != "BLOCK":
#                 full_raw_data += f"\nQuery: {q['text']}\n{result}\n"
#                 save_raw_log(company, q["text"], result)
#             else:
#                  print(f"      🔸 No data found.")

#             if current_mode == "LIBRARY_MODE":
#                 time.sleep(random.uniform(*SAFETY_MODE_DELAY))
#             else:
#                 time.sleep(random.uniform(*QUERY_GAP_DELAY))

#         if full_raw_data.strip():
#             print("      🧠 Analyzing...")
#             res = analyze_with_groq(company, full_raw_data)
#             final_data[company] = res
#             save_json(final_data)
#             print(f"      ✅ Saved: {json.dumps(res)}")
#         else:
#             final_data[company] = {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}
#             save_json(final_data)

#         cooldown = random.uniform(*COMPANY_COOLDOWN_DELAY)
#         print(f"[SLEEP] Cooling down for {cooldown:.1f}s...\n")
#         time.sleep(cooldown)

#     print("\n🎉 All Done!")

# def enrich_companies_from_list(company_list):
#     global TARGET_COMPANIES
#     TARGET_COMPANIES = list(set(company_list))
#     main()

# if __name__ == "__main__":
#     main()











#----------------------------


import json
import time
import random
import requests
import os
import datetime
from bs4 import BeautifulSoup
from groq import Groq
from fake_useragent import UserAgent
from duckduckgo_search import DDGS
from dotenv import load_dotenv
from itertools import cycle

# Load environment variables
load_dotenv()

# ==========================================
#  1. CONFIGURATION
# ==========================================

TARGET_COMPANIES = [
    "AnavClouds Software Solutions",
    "Fractal analytics",
    "Metacube",
    "AnavClouds analytics.ai",
    "Cyntexa"
]

FINAL_OUTPUT_FILE = "company_intel/Final_Company_Data_by_simple_approach.json"
RAW_DEBUG_FILE = "raw_search_logs_by_simple_approach.txt"

os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

# ------------------------------------------
# ⏱ TIMING & DELAY SETTINGS
# ------------------------------------------

HTML_MAX_RETRIES = 3
LIBRARY_MAX_RETRIES = 3
SAFETY_MODE_LIMIT = 10

# BLOCK_RETRY_DELAY = (150, 250)
# QUERY_GAP_DELAY = (70, 90)
# SAFETY_MODE_DELAY = (60, 120)
# COMPANY_COOLDOWN_DELAY = (100, 150)
BLOCK_RETRY_DELAY = (20, 40)
QUERY_GAP_DELAY = (5, 8)
SAFETY_MODE_DELAY = (15, 25)
COMPANY_COOLDOWN_DELAY = (10, 20)
# ------------------------------------------
#  API KEY LOADER (UNCHANGED)
# ------------------------------------------

def get_api_keys(prefix):
    keys = []
    i = 1
    while True:
        key = os.environ.get(f"{prefix}_{i}")
        if key:
            keys.append(key)
            i += 1
        else:
            break
    return keys

GROQ_KEYS = get_api_keys("GROQ_API_KEY")

if not GROQ_KEYS:
    raise RuntimeError("No GROQ API keys found in environment variables")

#  GLOBAL ROUND-ROBIN ROTATOR
GROQ_KEY_ROTATOR = cycle(GROQ_KEYS)

# ==========================================
#  2. SAFE SAVE FUNCTIONS
# ==========================================

def save_raw_log(company, query, raw_text):
    try:
        with open(RAW_DEBUG_FILE, "a", encoding="utf-8") as f:
            f.write(
                f"\n{'='*50}\n"
                f"Company: {company}\nQuery: {query}\n"
                f"{'-'*20}\n{raw_text}\n"
                f"{'='*50}\n"
            )
    except Exception as e:
        print(f" Raw log save error: {e}")

def save_json(data):
    try:
        with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f" JSON save error: {e}")

# ==========================================
#  3. SEARCH FUNCTIONS
# ==========================================

def search_via_html(query, time_filter=None):
    url = "https://html.duckduckgo.com/html/"
    headers = {
        "User-Agent": UserAgent().random,
        "Referer": "https://www.google.com/"
    }
    payload = {"q": query}
    if time_filter:
        payload["df"] = time_filter

    try:
        print(f"📡 HTML Search: {query}")
        response = requests.post(url, data=payload, headers=headers, timeout=15)

        if response.status_code in [403, 429] or "captcha" in response.text.lower():
            return "BLOCK"

        soup = BeautifulSoup(response.text, "html.parser")
        results = soup.find_all("div", class_="result__body", limit=10)

        if not results:
            return None

        text = ""
        for r in results:
            title = r.find("a", class_="result__a").get_text(strip=True)
            snippet = r.find("a", class_="result__snippet").get_text(strip=True)
            text += f"Source: {title}\nSnippet: {snippet}\n----------\n"

        return text.strip()

    except Exception:
        return "BLOCK"

def search_via_library(query, time_filter=None):
    try:
        timelimit = "y" if time_filter == "y" else None
        print(f" Library Search: {query}")

        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=10, timelimit=timelimit))
            if not results:
                return None

            text = ""
            for r in results:
                text += f"Source: {r.get('title')}\nSnippet: {r.get('body')}\n----------\n"

            return text.strip()

    except Exception:
        return "BLOCK"

# ==========================================
#  4. GROQ ANALYSIS (KEY ROTATION HERE)
# ==========================================

def analyze_with_groq(company, raw_data):
    """
    Uses ONE Groq API key per call.
    Automatically rotates keys using round-robin.
    """

    for _ in range(len(GROQ_KEYS)):
        try:
            api_key = next(GROQ_KEY_ROTATOR)
            client = Groq(api_key=api_key)

            completion = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Extract 'Annual Revenue' and 'Total Employee Count'. "
                            "Return JSON only: "
                            "{'Annual Revenue': '...', 'Total Employee Count': '...'}"
                        )
                    },
                    {
                        "role": "user",
                        "content": f"Company: {company}\nData:\n{raw_data}"
                    }
                ],
                response_format={"type": "json_object"}
            )

            return json.loads(completion.choices[0].message.content)

        except Exception as e:
            print(f" Groq key failed, rotating key... {e}")
            time.sleep(0.5)

    return {
        "Annual Revenue": "Not Found",
        "Total Employee Count": "Not Found"
    }

# ==========================================
#  5. MAIN PIPELINE
# ==========================================

def main():
    final_data = {}

    if os.path.exists(FINAL_OUTPUT_FILE):
        try:
            with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    final_data = json.loads(content)
        except Exception:
            final_data = {}

    prev_year = datetime.datetime.now().year - 1
    safety_mode_steps_remaining = 0

    print(f"🚀 Starting extraction for {len(TARGET_COMPANIES)} companies")

    for idx, company in enumerate(TARGET_COMPANIES, start=1):
        if company in final_data:
            print(f" Skipping {company}")
            continue

        current_mode = "LIBRARY_MODE" if safety_mode_steps_remaining > 0 else "HTML_MODE"
        safety_mode_steps_remaining = max(0, safety_mode_steps_remaining - 1)

        print(f"\n[{idx}] Processing: {company}")

        queries = [
            {"text": f"site:rocketreach.co {company} employee size", "filter": None},
            {"text": f"{company} annual revenue {prev_year}", "filter": "y"}
        ]

        raw_data = ""

        for q in queries:
            result = None

            if current_mode == "HTML_MODE":
                for _ in range(HTML_MAX_RETRIES):
                    result = search_via_html(q["text"], q["filter"])
                    if result != "BLOCK":
                        break
                    time.sleep(random.uniform(*BLOCK_RETRY_DELAY))

                if result == "BLOCK":
                    safety_mode_steps_remaining = SAFETY_MODE_LIMIT
                    current_mode = "LIBRARY_MODE"
                    result = None

            if current_mode == "LIBRARY_MODE":
                for _ in range(LIBRARY_MAX_RETRIES):
                    result = search_via_library(q["text"], q["filter"])
                    if result != "BLOCK":
                        break
                    time.sleep(random.uniform(*BLOCK_RETRY_DELAY))

            if result and result != "BLOCK":
                raw_data += f"\nQuery: {q['text']}\n{result}\n"
                save_raw_log(company, q["text"], result)

            time.sleep(
                random.uniform(*SAFETY_MODE_DELAY)
                if current_mode == "LIBRARY_MODE"
                else random.uniform(*QUERY_GAP_DELAY)
            )

        if raw_data.strip():
            result = analyze_with_groq(company, raw_data)
            final_data[company] = result
            save_json(final_data)
            print(f" Saved: {result}")
        else:
            final_data[company] = {
                "Annual Revenue": "Not Found",
                "Total Employee Count": "Not Found"
            }
            save_json(final_data)

        time.sleep(random.uniform(*COMPANY_COOLDOWN_DELAY))

    print("\n🎉 All companies processed successfully")


def enrich_companies_from_list(company_list):
    global TARGET_COMPANIES
    TARGET_COMPANIES = list(set(company_list))
    main()
# ==========================================
# 🟢 ENTRY POINT
# ==========================================

if __name__ == "__main__":
    main()


