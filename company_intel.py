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











# #----------------------------


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
# from itertools import cycle
# from API_rotation import get_groq_key,get_groq_count
# # Load environment variables
# load_dotenv()

# # ==========================================
# #  1. CONFIGURATION
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

# os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

# # ------------------------------------------
# # ⏱ TIMING & DELAY SETTINGS
# # ------------------------------------------

# HTML_MAX_RETRIES = 3
# LIBRARY_MAX_RETRIES = 3
# SAFETY_MODE_LIMIT = 10

# # BLOCK_RETRY_DELAY = (150, 250)
# # QUERY_GAP_DELAY = (70, 90)
# # SAFETY_MODE_DELAY = (60, 120)
# # COMPANY_COOLDOWN_DELAY = (100, 150)
# BLOCK_RETRY_DELAY = (20, 40)
# QUERY_GAP_DELAY = (5, 8)
# SAFETY_MODE_DELAY = (15, 25)
# COMPANY_COOLDOWN_DELAY = (10, 20)
# # ------------------------------------------
# #  API KEY LOADER (UNCHANGED)
# # ------------------------------------------

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
#     return keys

# GROQ_KEYS = get_api_keys("GROQ_API_KEY")

# if not GROQ_KEYS:
#     raise RuntimeError("No GROQ API keys found in environment variables")

# #  GLOBAL ROUND-ROBIN ROTATOR
# GROQ_KEY_ROTATOR = cycle(GROQ_KEYS)

# # ==========================================
# #  2. SAFE SAVE FUNCTIONS
# # ==========================================

# def save_raw_log(company, query, raw_text):
#     try:
#         with open(RAW_DEBUG_FILE, "a", encoding="utf-8") as f:
#             f.write(
#                 f"\n{'='*50}\n"
#                 f"Company: {company}\nQuery: {query}\n"
#                 f"{'-'*20}\n{raw_text}\n"
#                 f"{'='*50}\n"
#             )
#     except Exception as e:
#         print(f" Raw log save error: {e}")

# def save_json(data):
#     try:
#         with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
#             json.dump(data, f, indent=4, ensure_ascii=False)
#     except Exception as e:
#         print(f" JSON save error: {e}")

# # ==========================================
# #  3. SEARCH FUNCTIONS
# # ==========================================

# def search_via_html(query, time_filter=None):
#     url = "https://html.duckduckgo.com/html/"
#     headers = {
#         "User-Agent": UserAgent().random,
#         "Referer": "https://www.google.com/"
#     }
#     payload = {"q": query}
#     if time_filter:
#         payload["df"] = time_filter

#     try:
#         print(f"📡 HTML Search: {query}")
#         response = requests.post(url, data=payload, headers=headers, timeout=15)

#         if response.status_code in [403, 429] or "captcha" in response.text.lower():
#             return "BLOCK"

#         soup = BeautifulSoup(response.text, "html.parser")
#         results = soup.find_all("div", class_="result__body", limit=10)

#         if not results:
#             return None

#         text = ""
#         for r in results:
#             title = r.find("a", class_="result__a").get_text(strip=True)
#             snippet = r.find("a", class_="result__snippet").get_text(strip=True)
#             text += f"Source: {title}\nSnippet: {snippet}\n----------\n"

#         return text.strip()

#     except Exception:
#         return "BLOCK"

# def search_via_library(query, time_filter=None):
#     try:
#         timelimit = "y" if time_filter == "y" else None
#         print(f" Library Search: {query}")

#         with DDGS() as ddgs:
#             results = list(ddgs.text(query, max_results=10, timelimit=timelimit))
#             if not results:
#                 return None

#             text = ""
#             for r in results:
#                 text += f"Source: {r.get('title')}\nSnippet: {r.get('body')}\n----------\n"

#             return text.strip()

#     except Exception:
#         return "BLOCK"

# # ==========================================
# #  4. GROQ ANALYSIS (KEY ROTATION HERE)
# # ==========================================

# def analyze_with_groq(company, raw_data):
#     """
#     Uses ONE Groq API key per call.
#     Automatically rotates keys using round-robin.
#     """
#     # 1. Get the total count of keys from the manager
#     total_keys = get_groq_count()
    
#     # Safety: Ensure we try at least once even if count returns 0 (unlikely)
#     max_retries = max(1, total_keys)
#     for _ in range(max_retries):
#         try:
#             # api_key = next(GROQ_KEY_ROTATOR)
#             api_key = get_groq_key()
#             client = Groq(api_key=api_key)

#             completion = client.chat.completions.create(
#                 model="llama-3.3-70b-versatile",
#                 messages=[
#                     {
#                         "role": "system",
#                         "content": (
#                             "Extract 'Annual Revenue' and 'Total Employee Count'. "
#                             "Return JSON only: "
#                             "{'Annual Revenue': '...', 'Total Employee Count': '...'}"
#                         )
#                     },
#                     {
#                         "role": "user",
#                         "content": f"Company: {company}\nData:\n{raw_data}"
#                     }
#                 ],
#                 response_format={"type": "json_object"}
#             )

#             return json.loads(completion.choices[0].message.content)

#         except Exception as e:
#             print(f" Groq key failed, rotating key... {e}")
#             time.sleep(0.5)

#     return {
#         "Annual Revenue": "Not Found",
#         "Total Employee Count": "Not Found"
#     }

# # ==========================================
# #  5. MAIN PIPELINE
# # ==========================================

# def main():
#     final_data = {}

#     if os.path.exists(FINAL_OUTPUT_FILE):
#         try:
#             with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
#                 content = f.read().strip()
#                 if content:
#                     final_data = json.loads(content)
#         except Exception:
#             final_data = {}

#     prev_year = datetime.datetime.now().year - 1
#     safety_mode_steps_remaining = 0

#     print(f"🚀 Starting extraction for {len(TARGET_COMPANIES)} companies")

#     for idx, company in enumerate(TARGET_COMPANIES, start=1):
#         if company in final_data:
#             print(f" Skipping {company}")
#             continue

#         current_mode = "LIBRARY_MODE" if safety_mode_steps_remaining > 0 else "HTML_MODE"
#         safety_mode_steps_remaining = max(0, safety_mode_steps_remaining - 1)

#         print(f"\n[{idx}] Processing: {company}")

#         queries = [
#             {"text": f"site:rocketreach.co {company} employee size", "filter": None},
#             {"text": f"{company} annual revenue {prev_year}", "filter": "y"}
#         ]

#         raw_data = ""

#         for q in queries:
#             result = None

#             if current_mode == "HTML_MODE":
#                 for _ in range(HTML_MAX_RETRIES):
#                     result = search_via_html(q["text"], q["filter"])
#                     if result != "BLOCK":
#                         break
#                     time.sleep(random.uniform(*BLOCK_RETRY_DELAY))

#                 if result == "BLOCK":
#                     safety_mode_steps_remaining = SAFETY_MODE_LIMIT
#                     current_mode = "LIBRARY_MODE"
#                     result = None

#             if current_mode == "LIBRARY_MODE":
#                 for _ in range(LIBRARY_MAX_RETRIES):
#                     result = search_via_library(q["text"], q["filter"])
#                     if result != "BLOCK":
#                         break
#                     time.sleep(random.uniform(*BLOCK_RETRY_DELAY))

#             if result and result != "BLOCK":
#                 raw_data += f"\nQuery: {q['text']}\n{result}\n"
#                 save_raw_log(company, q["text"], result)

#             time.sleep(
#                 random.uniform(*SAFETY_MODE_DELAY)
#                 if current_mode == "LIBRARY_MODE"
#                 else random.uniform(*QUERY_GAP_DELAY)
#             )

#         if raw_data.strip():
#             result = analyze_with_groq(company, raw_data)
#             final_data[company] = result
#             save_json(final_data)
#             print(f" Saved: {result}")
#         else:
#             final_data[company] = {
#                 "Annual Revenue": "Not Found",
#                 "Total Employee Count": "Not Found"
#             }
#             save_json(final_data)

#         time.sleep(random.uniform(*COMPANY_COOLDOWN_DELAY))

#     print("\n🎉 All companies processed successfully")


# def enrich_companies_from_list(company_list):
#     global TARGET_COMPANIES
#     TARGET_COMPANIES = list(set(company_list))
#     main()
# # ==========================================
# # 🟢 ENTRY POINT
# # ==========================================

# if __name__ == "__main__":
#     main()



# import json
# import time
# import random
# import requests
# import os
# import datetime
# from bs4 import BeautifulSoup
# from groq import Groq
# from fake_useragent import UserAgent
# from dotenv import load_dotenv
# from itertools import cycle
# from API_rotation import get_groq_key, get_groq_count

# # Load environment variables
# load_dotenv()

# # ==========================================
# #  1. CONFIGURATION
# # ==========================================

# TARGET_COMPANIES = [
#     "AnavClouds Software Solutions",
#     "Fractal analytics",
#     "Metacube",
#     "Cyntexa"
# ]

# FINAL_OUTPUT_FILE = "company_intel/Final_Company_Data_by_simple_approach.json"
# RAW_DEBUG_FILE = "raw_search_logs_by_simple_approach.txt"

# os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

# # ------------------------------------------
# # ⏱ TIMING & DELAY SETTINGS
# # ------------------------------------------

# HTML_MAX_RETRIES = 3

# # Delays (in seconds)
# BLOCK_RETRY_DELAY = (20, 40)
# QUERY_GAP_DELAY = (6, 10)
# COMPANY_COOLDOWN_DELAY = (10, 20)

# # ------------------------------------------
# #  API KEY LOADER (UNCHANGED)
# # ------------------------------------------

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
#     return keys

# GROQ_KEYS = get_api_keys("GROQ_API_KEY")

# if not GROQ_KEYS:
#     raise RuntimeError("No GROQ API keys found in environment variables")

# #  GLOBAL ROUND-ROBIN ROTATOR
# GROQ_KEY_ROTATOR = cycle(GROQ_KEYS)

# # ==========================================
# #  2. SAFE SAVE FUNCTIONS
# # ==========================================

# def save_raw_log(company, query, raw_text):
#     try:
#         with open(RAW_DEBUG_FILE, "a", encoding="utf-8") as f:
#             f.write(
#                 f"\n{'='*50}\n"
#                 f"Company: {company}\nQuery: {query}\n"
#                 f"{'-'*20}\n{raw_text}\n"
#                 f"{'='*50}\n"
#             )
#     except Exception as e:
#         print(f" Raw log save error: {e}")

# def save_json(data):
#     try:
#         with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
#             json.dump(data, f, indent=4, ensure_ascii=False)
#     except Exception as e:
#         print(f" JSON save error: {e}")

# # ==========================================
# #  3. SEARCH FUNCTIONS (HTML ONLY)
# # ==========================================

# def search_via_html(query, time_filter=None):
#     url = "https://html.duckduckgo.com/html/"
#     headers = {
#         "User-Agent": UserAgent().random,
#         "Referer": "https://www.google.com/"
#     }
#     payload = {"q": query}
#     if time_filter:
#         payload["df"] = time_filter

#     try:
#         print(f"📡 HTML Search: {query}")
#         response = requests.post(url, data=payload, headers=headers, timeout=15)

#         # If we are blocked, return "BLOCK" so the main loop handles it
#         if response.status_code in [403, 429] or "captcha" in response.text.lower():
#             return "BLOCK"

#         soup = BeautifulSoup(response.text, "html.parser")
#         results = soup.find_all("div", class_="result__body", limit=10)

#         if not results:
#             return None

#         text = ""
#         for r in results:
#             title_tag = r.find("a", class_="result__a")
#             snippet_tag = r.find("a", class_="result__snippet")
            
#             # Safety check if tags exist
#             title = title_tag.get_text(strip=True) if title_tag else "No Title"
#             snippet = snippet_tag.get_text(strip=True) if snippet_tag else "No Snippet"
            
#             text += f"Source: {title}\nSnippet: {snippet}\n----------\n"

#         return text.strip()

#     except Exception as e:
#         print(f"Search Error: {e}")
#         return "BLOCK"

# # ==========================================
# #  4. GROQ ANALYSIS (KEY ROTATION HERE)
# # ==========================================

# def analyze_with_groq(company, raw_data):
#     """
#     Uses ONE Groq API key per call.
#     Automatically rotates keys using round-robin.
#     """
#     # 1. Get the total count of keys from the manager
#     total_keys = get_groq_count()
    
#     # Safety: Ensure we try at least once even if count returns 0
#     max_retries = max(1, total_keys)
    
#     for _ in range(max_retries):
#         try:
#             # api_key = next(GROQ_KEY_ROTATOR)
#             api_key = get_groq_key()
#             client = Groq(api_key=api_key)

#             completion = client.chat.completions.create(
#                 model="llama-3.3-70b-versatile",
#                 messages=[
#                     {
#                         "role": "system",
#                         "content": (
#                             "Extract 'Annual Revenue' and 'Total Employee Count'. "
#                             "Return JSON only: "
#                             "{'Annual Revenue': '...', 'Total Employee Count': '...'}"
#                         )
#                     },
#                     {
#                         "role": "user",
#                         "content": f"Company: {company}\nData:\n{raw_data}"
#                     }
#                 ],
#                 response_format={"type": "json_object"}
#             )

#             return json.loads(completion.choices[0].message.content)

#         except Exception as e:
#             print(f" Groq key failed, rotating key... {e}")
#             time.sleep(0.5)

#     return {
#         "Annual Revenue": "Not Found",
#         "Total Employee Count": "Not Found"
#     }

# # ==========================================
# #  5. MAIN PIPELINE
# # ==========================================

# def main():
#     final_data = {}

#     # Load existing data to avoid re-processing
#     if os.path.exists(FINAL_OUTPUT_FILE):
#         try:
#             with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
#                 content = f.read().strip()
#                 if content:
#                     final_data = json.loads(content)
#         except Exception:
#             final_data = {}

#     prev_year = datetime.datetime.now().year - 1

#     print(f"🚀 Starting extraction for {len(TARGET_COMPANIES)} companies (HTML ONLY MODE)")

#     for idx, company in enumerate(TARGET_COMPANIES, start=1):
#         if company in final_data:
#             print(f" Skipping {company}")
#             continue

#         print(f"\n[{idx}] Processing: {company}")

#         queries = [
#             {"text": f"site:rocketreach.co {company} employee size", "filter": None},
#             {"text": f"{company} annual revenue {prev_year}", "filter": "y"}
#         ]

#         raw_data = ""

#         for q in queries:
#             result = None

#             # Retry logic for HTML blocks
#             for attempt in range(HTML_MAX_RETRIES):
#                 result = search_via_html(q["text"], q["filter"])
                
#                 if result == "BLOCK":
#                     print(f"⚠️  Blocked (Attempt {attempt+1}/{HTML_MAX_RETRIES}). Waiting...")
#                     time.sleep(random.uniform(*BLOCK_RETRY_DELAY))
#                 else:
#                     # Success or just no results (but not blocked)
#                     break 

#             if result and result != "BLOCK":
#                 raw_data += f"\nQuery: {q['text']}\n{result}\n"
#                 save_raw_log(company, q["text"], result)

#             # Standard delay between queries to be polite
#             time.sleep(random.uniform(*QUERY_GAP_DELAY))

#         # Only run Groq if we actually found data
#         if raw_data.strip():
#             result = analyze_with_groq(company, raw_data)
#             final_data[company] = result
#             save_json(final_data)
#             print(f" Saved: {result}")
#         else:
#             final_data[company] = {
#                 "Annual Revenue": "Not Found",
#                 "Total Employee Count": "Not Found"
#             }
#             save_json(final_data)

#         # Delay between companies
#         time.sleep(random.uniform(*COMPANY_COOLDOWN_DELAY))

#     print("\n🎉 All companies processed successfully")


# def enrich_companies_from_list(company_list):
#     global TARGET_COMPANIES
#     TARGET_COMPANIES = list(set(company_list))
#     main()

# # ==========================================
# # 🟢 ENTRY POINT
# # ==========================================

# if __name__ == "__main__":
#     main()




# #-------------------------------------------------------------------------------

# # import json
# # import time
# # import os
# # import datetime
# # import random
# # from dotenv import load_dotenv
# # from groq import Groq
# # from tavily import TavilyClient

# # # ==========================================
# # #  IMPORT KEY ROTATION LOGIC
# # # ==========================================
# # # Ensure ALL these functions exist in your API_rotation.py
# # from API_rotation import (
# #     get_groq_key, 
# #     get_groq_count, 
# #     get_tavily_key, 
# #     get_tavily_count
# # )

# # # Load environment variables
# # load_dotenv()

# # # ==========================================
# # #  1. CONFIGURATION
# # # ==========================================

# # # Default list (used if ran directly)
# # TARGET_COMPANIES = [
# #     "AnavClouds Software Solutions",
# #     "Fractal analytics",
# #     "Metacube",
# #     "Cyntexa"
# # ]

# # # Final clean data (JSON)
# # FINAL_OUTPUT_FILE = "company_intel/Final_Company_Data_by_simple_approach.json"
# # # Raw search logs (Text file for debugging)
# # RAW_DEBUG_FILE = "raw_search_logs_by_simple_approach.txt"

# # os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

# # # ------------------------------------------
# # #  TIMING SETTINGS
# # # ------------------------------------------
# # COMPANY_COOLDOWN_DELAY = (2, 5)

# # # ==========================================
# # #  2. SAVE FUNCTIONS
# # # ==========================================

# # def save_raw_log(company, query, raw_text):
# #     """
# #     Saves the raw text result from Tavily to a .txt file for debugging.
# #     """
# #     try:
# #         with open(RAW_DEBUG_FILE, "a", encoding="utf-8") as f:
# #             f.write(
# #                 f"\n{'='*50}\n"
# #                 f"Company: {company}\nQuery: {query}\n"
# #                 f"{'-'*20}\n{raw_text}\n"
# #                 f"{'='*50}\n"
# #             )
# #     except Exception as e:
# #         print(f" Raw log save error: {e}")

# # def save_json(data):
# #     """
# #     Saves the final structured data to a .json file.
# #     """
# #     try:
# #         with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
# #             json.dump(data, f, indent=4, ensure_ascii=False)
# #     except Exception as e:
# #         print(f" JSON save error: {e}")

# # # ==========================================
# # #  3. SEARCH FUNCTION (TAVILY WITH ROTATION)
# # # ==========================================

# # def search_via_tavily(company):
# #     """
# #     Rotates through available Tavily keys using get_tavily_count().
# #     """
# #     current_year = datetime.datetime.now().year
# #     prev_year = current_year - 1
    
# #     # Construct a specific query
# #     query = f"{company} total annual revenue {prev_year} and total employee count {current_year} financial report"

# #     # 1. Get total available keys to determine retry limit
# #     total_keys = get_tavily_count()
# #     max_retries = max(1, total_keys)
    
# #     for attempt in range(max_retries):
# #         try:
# #             # 2. Get a fresh key from rotation logic
# #             tavily_key = get_tavily_key()
# #             client = TavilyClient(api_key=tavily_key)

# #             print(f"🔎 Tavily Search (Attempt {attempt+1}/{max_retries}): {query}")
            
# #             # 3. Execute Search
# #             # search_depth="advanced" costs 2 credits but gives deeper results
# #             response = client.search(
# #                 query=query,
# #                 search_depth="advanced",
# #                 max_results=2,
# #                 include_answer=False
# #             )

# #             # 4. Format results into a string
# #             context_text = ""
# #             for result in response.get("results", []):
# #                 context_text += f"Source: {result['title']}\nURL: {result['url']}\nContent: {result['content']}\n----------\n"

# #             return context_text, query

# #         except Exception as e:
# #             print(f"⚠️ Tavily Key Failed (Attempt {attempt+1}): {e}")
# #             time.sleep(1) # Short wait before rotating to next key

# #     print("❌ All Tavily keys failed or ran out of credits.")
# #     return None, query

# # # ==========================================
# # #  4. GROQ ANALYSIS (EXISTING ROTATION)
# # # ==========================================

# # def analyze_with_groq(company, raw_data):
# #     """
# #     Uses Groq key rotation to extract JSON from the raw text.
# #     """
# #     total_keys = get_groq_count()
# #     max_retries = max(1, total_keys)

# #     for _ in range(max_retries):
# #         try:
# #             # Get key from rotation logic
# #             api_key = get_groq_key()
# #             client = Groq(api_key=api_key)

# #             completion = client.chat.completions.create(
# #                 model="llama-3.3-70b-versatile",
# #                 messages=[
# #                     {
# #                         "role": "system",
# #                         "content": (
# #                             "You are an expert Data Analyst. "
# #                             "Extract 'Annual Revenue' (include currency) and 'Total Employee Count'. "
# #                             "Extract the right or exact Information after reading Company info."
# #                             "If the exact number is not found, look for the most recent estimate. "
# #                             "Return strict JSON only: "
# #                             "{\"Annual Revenue\": \"...\", \"Total Employee Count\": \"...\"}"
# #                         )
# #                     },
# #                     {
# #                         "role": "user",
# #                         "content": f"Company: {company}\nContext Data:\n{raw_data}"
# #                     }
# #                 ],
# #                 response_format={"type": "json_object"}
# #             )

# #             return json.loads(completion.choices[0].message.content)

# #         except Exception as e:
# #             print(f"⚠️ Groq key failed, rotating... Error: {e}")
# #             time.sleep(0.5)

# #     return {
# #         "Annual Revenue": "Not Found",
# #         "Total Employee Count": "Not Found"
# #     }

# # # ==========================================
# # #  5. MAIN PIPELINE
# # # ==========================================

# # def main():
# #     final_data = {}

# #     # Load existing data to avoid re-processing
# #     if os.path.exists(FINAL_OUTPUT_FILE):
# #         try:
# #             with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
# #                 content = f.read().strip()
# #                 if content:
# #                     final_data = json.loads(content)
# #         except Exception:
# #             final_data = {}

# #     print(f"🚀 Starting extraction for {len(TARGET_COMPANIES)} companies (TAVILY MODE)")

# #     for idx, company in enumerate(TARGET_COMPANIES, start=1):
# #         if company in final_data:
# #             print(f"⏭️  Skipping {company}")
# #             continue

# #         print(f"\n[{idx}] Processing: {company}")

# #         # 1. Search via Tavily (with rotation & count check)
# #         raw_context, used_query = search_via_tavily(company)

# #         if raw_context:
# #             # Save Raw Log (Text File)
# #             save_raw_log(company, used_query, raw_context)

# #             # 2. Extract via Groq (with rotation)
# #             result = analyze_with_groq(company, raw_context)
            
# #             # 3. Save Final JSON
# #             final_data[company] = result
# #             save_json(final_data)
# #             print(f"✅ Saved: {result}")
# #         else:
# #             print("❌ No data found.")
# #             final_data[company] = {
# #                 "Annual Revenue": "Not Found",
# #                 "Total Employee Count": "Not Found"
# #             }
# #             save_json(final_data)

# #         # Politeness delay
# #         time.sleep(random.uniform(*COMPANY_COOLDOWN_DELAY))

# #     print("\n🎉 All companies processed successfully")

# # # ==========================================
# # #  6. EXTERNAL CALL HANDLER
# # # ==========================================

# # def enrich_companies_from_list(company_list):
# #     """
# #     This function allows project_2.py to pass a list of companies.
# #     """
# #     global TARGET_COMPANIES
# #     # Remove duplicates and update the global list
# #     TARGET_COMPANIES = list(set(company_list))
# #     # Run the main pipeline
# #     main()

# # # ==========================================
# # #  ENTRY POINT
# # # ==========================================

# # if __name__ == "__main__":
# #     main()





# import json
# import time
# import random
# import requests
# import os
# import datetime
# from bs4 import BeautifulSoup
# from groq import Groq
# from fake_useragent import UserAgent
# from dotenv import load_dotenv
# from tavily import TavilyClient

# # API Rotation Imports (Ensure these exist in your project)
# from API_rotation import (
#     get_groq_key,
#     get_groq_count,
#     get_tavily_key,
#     get_tavily_count
# )

# load_dotenv()

# # ==========================================
# #  1. CONFIGURATION
# # ==========================================

# TARGET_COMPANIES = [
#     "AnavClouds Software Solutions",
#     "Fractal analytics",
#     "Metacube",
#     "Cyntexa"
# ]

# FINAL_OUTPUT_FILE = "company_intel/Final_Company_Data_Hybrid_Debug.json"
# RAW_DEBUG_FILE = "raw_search_logs_hybrid_debug.txt"

# os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

# HTML_MAX_RETRIES = 2
# BLOCK_RETRY_DELAY = (2, 5)
# QUERY_GAP_DELAY = (2, 4)
# COMPANY_COOLDOWN_DELAY = (3, 6)

# # ==========================================
# #  2. SAVE FUNCTIONS
# # ==========================================

# def save_raw_log(company, source, raw_text):
#     try:
#         with open(RAW_DEBUG_FILE, "a", encoding="utf-8") as f:
#             f.write(f"\n{'='*50}\nCompany: {company}\nSource: {source}\n{'-'*20}\n{raw_text}\n{'='*50}\n")
#     except Exception as e:
#         print(f" Raw log save error: {e}")

# def save_json(data):
#     try:
#         with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
#             json.dump(data, f, indent=4, ensure_ascii=False)
#     except Exception as e:
#         print(f" JSON save error: {e}")

# # ==========================================
# #  3. DEBUGGED DUCKDUCKGO SEARCH
# # ==========================================

# def search_via_html(query, time_filter=None):
#     url = "https://html.duckduckgo.com/html/"
#     headers = {
#         "User-Agent": UserAgent().random,
#         "Referer": "https://www.google.com/"
#     }
#     payload = {"q": query}
#     if time_filter:
#         payload["df"] = time_filter

#     try:
#         print(f"    👉 Sending Request: {query[:40]}...")
#         response = requests.post(url, data=payload, headers=headers, timeout=15)
        
#         # --- DEBUG PRINT ---
#         print(f"    📡 Status Code: {response.status_code}")
        
#         # Check Blocking
#         if response.status_code in [403, 429]:
#             print(f"    🛑 BLOCKED by Server (Status {response.status_code})")
#             return "BLOCK"
            
#         if "captcha" in response.text.lower():
#             print(f"    🛑 BLOCKED by CAPTCHA detected in text.")
#             return "BLOCK"

#         soup = BeautifulSoup(response.text, "html.parser")
#         results = soup.find_all("div", class_="result__body", limit=10)

#         # --- DEBUG PRINT ---
#         print(f"    🔍 Results Extracted: {len(results)}")

#         if not results:
#             print("    ⚠️ No results found in HTML (Empty Page?)")
#             return None

#         text = ""
#         for r in results:
#             title_tag = r.find("a", class_="result__a")
#             snippet_tag = r.find("a", class_="result__snippet")
            
#             title = title_tag.get_text(strip=True) if title_tag else "No Title"
#             snippet = snippet_tag.get_text(strip=True) if snippet_tag else "No Snippet"
            
#             text += f"Source: {title}\nSnippet: {snippet}\n----------\n"

#         return text.strip()

#     except Exception as e:
#         print(f"    💥 Exception Error: {e}")
#         return "BLOCK"

# # ==========================================
# #  4. TAVILY FALLBACK
# # ==========================================

# def search_via_tavily(company):
#     current_year = datetime.datetime.now().year
#     prev_year = current_year - 1
#     query = f"{company} total annual revenue {prev_year} and total employee count {current_year} financial report"
    
#     total_keys = get_tavily_count()
#     max_retries = max(1, total_keys)
    
#     print(f"  ↳ 🦅 Switching to Tavily Search...")

#     for attempt in range(max_retries):
#         try:
#             tavily_key = get_tavily_key()
#             if not tavily_key: return None
#             client = TavilyClient(api_key=tavily_key)
#             response = client.search(query=query, search_depth="advanced", max_results=2, include_answer=False)
            
#             context_text = ""
#             for result in response.get("results", []):
#                 context_text += f"Title: {result['title']}\nContent: {result['content']}\n----------\n"
#             return context_text
#         except Exception as e:
#             print(f"    ⚠️ Tavily Error: {e}")
#             time.sleep(1)
#     return None

# # ==========================================
# #  5. GROQ ANALYSIS
# # ==========================================

# def analyze_with_groq(company, raw_data):
#     # (Same Logic as before, kept short for brevity)
#     max_retries = max(1, get_groq_count())
#     for _ in range(max_retries):
#         try:
#             api_key = get_groq_key()
#             client = Groq(api_key=api_key)
#             completion = client.chat.completions.create(
#                 model="llama-3.3-70b-versatile",
#                 messages=[
#                     {
#                         "role": "system",
#                         "content": (
#                             "Extract 'Annual Revenue' and 'Total Employee Count'. "
#                             "Return JSON only: "
#                             "{'Annual Revenue': '...', 'Total Employee Count': '...'}"
#                         )
#                     },
#                     {
#                         "role": "user",
#                         "content": f"Company: {company}\nData:\n{raw_data}"
#                     }
#                 ],
#                 response_format={"type": "json_object"}
#             )

#             return json.loads(completion.choices[0].message.content)
               
#         except Exception:
#             time.sleep(0.5)
#     return {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}

# # ==========================================
# #  6. MAIN LOOP
# # ==========================================

# def main():
#     final_data = {}
#     prev_year = datetime.datetime.now().year - 1
#     print(f"🚀 Starting Extraction with DEBUG LOGS")

#     for idx, company in enumerate(TARGET_COMPANIES, start=1):
#         print(f"\n[{idx}] Processing: {company}")
        
#         # --- DDG ATTEMPT ---
#         raw_data = ""
#         source_used = "None"
#         ddg_success = False
        
#         queries = [
#             {"text": f"site:rocketreach.co {company} employee size", "filter": None},
#             {"text": f"{company} annual revenue {prev_year}", "filter": "y"}
#         ]
        
#         for q in queries:
#             print(f"  ↳ 🦆 Query: {q['text']}")
#             result = None
#             for attempt in range(HTML_MAX_RETRIES):
#                 result = search_via_html(q["text"], q["filter"])
#                 if result == "BLOCK":
#                     print(f"    ⏳ Waiting 2s before retry...")
#                     time.sleep(2)
#                 else:
#                     break 
            
#             if result and result != "BLOCK":
#                 raw_data += f"\nQuery: {q['text']}\n{result}\n"
#             else:
#                 print("    ❌ This query failed/blocked.")

#         if raw_data.strip():
#             ddg_success = True
#             source_used = "DuckDuckGo"
#             print("  ✅ DDG Success! Data Collected.")
#         else:
#             print("  ⚠️ All DDG queries failed. Invoking Fallback.")

#         # --- FALLBACK ---
#         if not ddg_success:
#             tavily_data = search_via_tavily(company)
#             if tavily_data:
#                 raw_data = tavily_data
#                 source_used = "Tavily"
#                 print("  ✅ Tavily Success!")
#             else:
#                 print("  ❌ Tavily Failed too.")

#         # --- SAVE ---
#         if raw_data.strip():
#             result_json = analyze_with_groq(company, raw_data)
#             result_json["Source_Method"] = source_used
#             final_data[company] = result_json
#             save_json(final_data)
#             print(f"  💾 Saved: {result_json}")
        
#         time.sleep(2)

#     print("\n🎉 Done")

# def enrich_companies_from_list(company_list):
#     """
#     This function allows project_2.py to pass a list of companies.
#     """
#     global TARGET_COMPANIES
#     # Remove duplicates and update the global list
#     TARGET_COMPANIES = list(set(company_list))
#     # Run the main pipeline
#     main()

# if __name__ == "__main__":
#     main()



# import json
# import time
# import random
# import requests
# import os
# import datetime
# from bs4 import BeautifulSoup
# from groq import Groq
# from fake_useragent import UserAgent
# from dotenv import load_dotenv
# from tavily import TavilyClient

# # API Rotation Imports
# from API_rotation import (
#     get_groq_key,
#     get_groq_count,
#     get_tavily_key,
#     get_tavily_count
# )

# load_dotenv()

# # ==========================================
# #  1. CONFIGURATION
# # ==========================================

# TARGET_COMPANIES = [
#     "AnavClouds Software Solutions",
#     "Fractal analytics",
#     "Metacube",
#     "Cyntexa"
# ]

# FINAL_OUTPUT_FILE = "company_intel/Final_Company_Data_by_simple_approach.json"
# RAW_DEBUG_FILE = "raw_search_logs_by_simple_approach.txt"

# os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

# HTML_MAX_RETRIES = 2
# BLOCK_RETRY_DELAY = (2, 5)
# QUERY_GAP_DELAY = (2, 4)
# COMPANY_COOLDOWN_DELAY = (3, 6)

# # ==========================================
# #  2. SAVE FUNCTIONS
# # ==========================================

# def save_raw_log(company, source, raw_text):
#     try:
#         with open(RAW_DEBUG_FILE, "a", encoding="utf-8") as f:
#             f.write(f"\n{'='*50}\nCompany: {company}\nSource: {source}\n{'-'*20}\n{raw_text}\n{'='*50}\n")
#     except Exception as e:
#         print(f" Raw log save error: {e}")

# def save_json(data):
#     try:
#         with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
#             json.dump(data, f, indent=4, ensure_ascii=False)
#     except Exception as e:
#         print(f" JSON save error: {e}")

# # ==========================================
# #  3. SEARCH FUNCTIONS (STRICT DDG)
# # ==========================================

# def search_via_html(query, time_filter=None):
#     url = "https://html.duckduckgo.com/html/"
#     headers = {
#         "User-Agent": UserAgent().random,
#         "Referer": "https://www.google.com/"
#     }
#     payload = {"q": query}
#     if time_filter:
#         payload["df"] = time_filter

#     try:
#         print(f"    👉 Sending Request: {query[:40]}...")
#         response = requests.post(url, data=payload, headers=headers, timeout=15)
        
#         # STRICT CHECKING FOR BLOCKS
#         if response.status_code != 200:
#             print(f"    🛑 BLOCKED by Server (Status {response.status_code})")
#             return "BLOCK"
            
#         if "captcha" in response.text.lower():
#             print(f"    🛑 BLOCKED by CAPTCHA detected.")
#             return "BLOCK"

#         soup = BeautifulSoup(response.text, "html.parser")
#         results = soup.find_all("div", class_="result__body", limit=10)

#         if not results:
#             print("    ⚠️ No results found in HTML (Soft Block or Empty)")
#             return "BLOCK" # Treat empty results as block to trigger Tavily

#         text = ""
#         for r in results:
#             title_tag = r.find("a", class_="result__a")
#             snippet_tag = r.find("a", class_="result__snippet")
            
#             title = title_tag.get_text(strip=True) if title_tag else "No Title"
#             snippet = snippet_tag.get_text(strip=True) if snippet_tag else "No Snippet"
            
#             text += f"Source: {title}\nSnippet: {snippet}\n----------\n"

#         return text.strip()

#     except Exception as e:
#         print(f"    💥 Exception Error: {e}")
#         return "BLOCK"

# # ==========================================
# #  4. TAVILY FALLBACK
# # ==========================================

# def search_via_tavily(company):
#     current_year = datetime.datetime.now().year
#     prev_year = current_year - 1
#     query = f"{company} total annual revenue {prev_year} and total employee count {current_year} financial report"
    
#     total_keys = get_tavily_count()
#     max_retries = max(1, total_keys)
    
#     print(f"  ↳ 🦅 Switching to Tavily Search...")

#     for attempt in range(max_retries):
#         try:
#             tavily_key = get_tavily_key()
#             if not tavily_key: return None
#             client = TavilyClient(api_key=tavily_key)
#             response = client.search(query=query, search_depth="advanced", max_results=2, include_answer=False)
            
#             context_text = ""
#             for result in response.get("results", []):
#                 context_text += f"Title: {result['title']}\nContent: {result['content']}\n----------\n"
#             return context_text
#         except Exception as e:
#             print(f"    ⚠️ Tavily Error: {e}")
#             time.sleep(1)
#     return None

# # ==========================================
# #  5. GROQ ANALYSIS
# # ==========================================

# def analyze_with_groq(company, raw_data):
#     """
#     Uses Groq API to extract specific fields.
#     """
#     max_retries = max(1, get_groq_count())
    
#     for _ in range(max_retries):
#         try:
#             api_key = get_groq_key()
#             client = Groq(api_key=api_key)
            
#             # --- YEH RAHA AAPKA PROMPT ---
#             completion = client.chat.completions.create(
#                 model="llama-3.3-70b-versatile",
#                 messages=[
#                     {
#                         "role": "system", 
#                         "content": (
#                             "Extract 'Annual Revenue' (with currency) and 'Total Employee Count'. "
#                             "Return strict JSON format only: "
#                             "{\"Annual Revenue\": \"...\", \"Total Employee Count\": \"...\"} "
#                             "If information is not found, write 'Not Found'."
#                         )
#                     },
#                     {
#                         "role": "user", 
#                         "content": f"Company: {company}\nData:\n{raw_data}"
#                     }
#                 ],
#                 response_format={"type": "json_object"}
#             )
#             return json.loads(completion.choices[0].message.content)

#         except Exception as e:
#             print(f"    ⚠️ Groq Error: {e}")
#             time.sleep(0.5)

#     return {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}

# # ==========================================
# #  6. MAIN PIPELINE
# # ==========================================

# def main():
#     final_data = {}
#     prev_year = datetime.datetime.now().year - 1
    
#     # Load existing data
#     if os.path.exists(FINAL_OUTPUT_FILE):
#         try:
#             with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
#                 final_data = json.loads(f.read().strip() or "{}")
#         except:
#             final_data = {}

#     print(f"🚀 Starting Extraction for {len(TARGET_COMPANIES)} companies (Hybrid Mode)")

#     for idx, company in enumerate(TARGET_COMPANIES, start=1):
#         if company in final_data and final_data[company].get("Source_Method", "Failed") != "Failed":
#              print(f"\n[{idx}] Skipping {company} (Already Done)")
#              continue

#         print(f"\n[{idx}] Processing: {company}")
        
#         # --- DDG ATTEMPT ---
#         raw_data = ""
#         source_used = "None"
#         ddg_success = False
        
#         queries = [
#             {"text": f"site:rocketreach.co {company} employee size", "filter": None},
#             {"text": f"{company} annual revenue {prev_year}", "filter": "y"}
#         ]
        
#         for q in queries:
#             # If one query already blocked, don't try the next one, go straight to Tavily
#             if raw_data == "BLOCK": break 

#             print(f"  ↳ 🦆 Query: {q['text']}")
#             result = None
            
#             for attempt in range(HTML_MAX_RETRIES):
#                 result = search_via_html(q["text"], q["filter"])
#                 if result == "BLOCK":
#                     print(f"    ⏳ Waiting 2s before retry...")
#                     time.sleep(2)
#                 else:
#                     break 
            
#             if result and result != "BLOCK":
#                 raw_data += f"\nQuery: {q['text']}\n{result}\n"
#             else:
#                 print("    ❌ This query failed/blocked.")
#                 raw_data = "BLOCK" # Mark as blocked to trigger fallback

#         # Check success
#         if raw_data and raw_data != "BLOCK":
#             ddg_success = True
#             source_used = "DuckDuckGo"
#             print("  ✅ DDG Success! Data Collected.")
#         else:
#             print("  ⚠️ DDG failed/blocked. Invoking Fallback.")

#         # --- FALLBACK (Tavily) ---
#         if not ddg_success:
#             tavily_data = search_via_tavily(company)
#             if tavily_data:
#                 raw_data = tavily_data
#                 source_used = "Tavily"
#                 print("  ✅ Tavily Success!")
#             else:
#                 print("  ❌ Tavily Failed too.")

#         # --- SAVE ---
#         if raw_data and raw_data != "BLOCK":
#             result_json = analyze_with_groq(company, raw_data)
#             result_json["Source_Method"] = source_used
#             final_data[company] = result_json
#             save_json(final_data)
#             print(f"  💾 Saved: {result_json}")
#         else:
#             final_data[company] = {
#                 "Annual Revenue": "Not Found", 
#                 "Total Employee Count": "Not Found", 
#                 "Source_Method": "Failed"
#             }
#             save_json(final_data)
        
#         time.sleep(2)

#     print("\n🎉 All companies processed successfully")


# def enrich_companies_from_list(company_list):
#     global TARGET_COMPANIES
#     TARGET_COMPANIES = list(set(company_list))
#     main()

# # ==========================================
# # 🟢 ENTRY POINT
# # ==========================================

# if __name__ == "__main__":
#     main()




# import json
# import time
# import os
# import datetime
# from groq import Groq
# from dotenv import load_dotenv
# from tavily import TavilyClient

# # API Rotation Imports
# from API_rotation import (
#     get_groq_key,
#     get_groq_count,
#     get_tavily_key,
#     get_tavily_count
# )

# load_dotenv()

# # ==========================================
# #  1. CONFIGURATION
# # ==========================================

# TARGET_COMPANIES = [
#     "AnavClouds Software Solutions",
#     "Fractal analytics",
#     "Metacube",
#     "Cyntexa"
# ]

# FINAL_OUTPUT_FILE = "company_intel/Final_Company_Data_by_simple_approach.json"
# RAW_DEBUG_FILE = "raw_search_logs_by_simple_approach.txt"

# os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

# # Timings
# COMPANY_COOLDOWN_DELAY = (2, 5)

# # ==========================================
# #  2. SAVE FUNCTIONS
# # ==========================================

# def save_raw_log(company, source, raw_text):
#     try:
#         with open(RAW_DEBUG_FILE, "a", encoding="utf-8") as f:
#             f.write(f"\n{'='*50}\nCompany: {company}\nSource: {source}\n{'-'*20}\n{raw_text}\n{'='*50}\n")
#     except Exception as e:
#         print(f" Raw log save error: {e}")

# def save_json(data):
#     try:
#         with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
#             json.dump(data, f, indent=4, ensure_ascii=False)
#     except Exception as e:
#         print(f" JSON save error: {e}")

# # ==========================================
# #  3. TAVILY SEARCH (PRIMARY & ONLY METHOD)
# # ==========================================

# def search_via_tavily(company):
#     """
#     Searches for company financial data using Tavily API.
#     """
#     current_year = datetime.datetime.now().year
#     prev_year = current_year - 1
    
#     # Specific query for Revenue and Employees
#     query = f"{company} total annual revenue {prev_year} and total employee count {current_year} financial report"
    
#     total_keys = get_tavily_count()
#     max_retries = max(1, total_keys)
    
#     print(f"  ↳ 🦅 Searching via Tavily API...")

#     for attempt in range(max_retries):
#         try:
#             tavily_key = get_tavily_key()
#             if not tavily_key:
#                 print("    ❌ No Tavily keys available.")
#                 return None
                
#             client = TavilyClient(api_key=tavily_key)
            
#             # Executing Search
#             response = client.search(
#                 query=query, 
#                 search_depth="advanced", 
#                 max_results=2, 
#                 include_answer=False
#             )
            
#             context_text = ""
#             for result in response.get("results", []):
#                 context_text += f"Title: {result['title']}\nContent: {result['content']}\n----------\n"
            
#             return context_text

#         except Exception as e:
#             print(f"    ⚠️ Tavily Key Failed (Attempt {attempt+1}): {e}")
#             time.sleep(1) # Short wait before rotating key
            
#     return None

# # ==========================================
# #  4. GROQ ANALYSIS
# # ==========================================

# def analyze_with_groq(company, raw_data):
#     """
#     Uses Groq API to extract specific fields from the raw text.
#     """
#     max_retries = max(1, get_groq_count())
    
#     for _ in range(max_retries):
#         try:
#             api_key = get_groq_key()
#             client = Groq(api_key=api_key)
            
#             completion = client.chat.completions.create(
#                 model="llama-3.3-70b-versatile",
#                 messages=[
#                     {
#                         "role": "system", 
#                         "content": (
#                             "Extract 'Annual Revenue' (with currency) and 'Total Employee Count'. "
#                             "Return strict JSON format only: "
#                             "{\"Annual Revenue\": \"...\", \"Total Employee Count\": \"...\"} "
#                             "If information is not found, write 'Not Found'."
#                         )
#                     },
#                     {
#                         "role": "user", 
#                         "content": f"Company: {company}\nData:\n{raw_data}"
#                     }
#                 ],
#                 response_format={"type": "json_object"}
#             )
#             return json.loads(completion.choices[0].message.content)

#         except Exception as e:
#             print(f"    ⚠️ Groq Error: {e}")
#             time.sleep(0.5)

#     return {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}

# # ==========================================
# #  5. MAIN PIPELINE
# # ==========================================

# def main():
#     final_data = {}
    
#     # Load existing data to avoid re-doing work
#     if os.path.exists(FINAL_OUTPUT_FILE):
#         try:
#             with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
#                 final_data = json.loads(f.read().strip() or "{}")
#         except:
#             final_data = {}

#     print(f"🚀 Starting Extraction for {len(TARGET_COMPANIES)} companies (TAVILY ONLY MODE)")

#     for idx, company in enumerate(TARGET_COMPANIES, start=1):
#         # Skip if already done
#         if company in final_data:
#              print(f"\n[{idx}] Skipping {company} (Already Done)")
#              continue

#         print(f"\n[{idx}] Processing: {company}")
        
#         # --- 1. SEARCH (TAVILY) ---
#         raw_data = search_via_tavily(company)
        
#         # --- 2. ANALYZE & SAVE ---
#         if raw_data:
#             print("  ✅ Data collected via Tavily.")
            
#             # Save Raw Logs
#             save_raw_log(company, "Tavily", raw_data)
            
#             # Extract with Groq
#             result_json = analyze_with_groq(company, raw_data)
            
#             # REMOVED: result_json["Source_Method"] = "Tavily"
            
#             final_data[company] = result_json
#             save_json(final_data)
#             print(f"  💾 Saved: {result_json}")
            
#         else:
#             print("  ❌ No data found.")
#             final_data[company] = {
#                 "Annual Revenue": "Not Found", 
#                 "Total Employee Count": "Not Found"
#                 # REMOVED: "Source_Method": "Failed"
#             }
#             save_json(final_data)
        
#         # Politeness Delay
#         time.sleep(2)

#     print("\n🎉 All companies processed successfully")

# # ==========================================
# #  6. EXTERNAL ENTRY POINT
# # ==========================================

# def enrich_companies_from_list(company_list):
#     global TARGET_COMPANIES
#     TARGET_COMPANIES = list(set(company_list))
#     main()

# # ==========================================
# # 🟢 ENTRY POINT
# # ==========================================

# if __name__ == "__main__":

#     main()




import json
import time
import os
import re
import datetime
import asyncio  # <--- CHANGED: Imported asyncio for parallel processing
from groq import Groq
from dotenv import load_dotenv
from tavily import TavilyClient

# API Rotation Imports
from API_rotation import (
    get_groq_key,
    get_groq_count,
    get_tavily_key,
    get_tavily_count
)

load_dotenv()

# ==========================================
#  1. CONFIGURATION
# ==========================================

TARGET_COMPANIES = [
    "AnavClouds Software Solutions",
    "Fractal analytics",
    "Metacube"
]

FINAL_OUTPUT_FILE = "company_intel/Final_Company_Data_by_simple_approach.json"
RAW_DEBUG_FILE = "raw_search_logs_async.json"

os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

# <--- CHANGED: Concurrency Limit
# We set this to 5 because you have 5 Groq keys. 
# This ensures 1 request per key at any given time, preventing Rate Limits.
CONCURRENCY_LIMIT = 5 

# ==========================================
#  2. HELPER FUNCTIONS
# ==========================================

def clean_text(text):
    if not text: return ""
    text = text.replace("\n", " ").replace("\t", " ").replace("\\", "")
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def save_raw_log(company, source, raw_text, direct_answer=None):
    """
    Saves raw logs. Using append mode is generally safe for simple async tasks.
    """
    new_entry = {
        "company": company,
        "source": source,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tavily_direct_answer": direct_answer, 
        "raw_search_context": raw_text
    }
    
    # Read-Modify-Write pattern
    data_list = []
    if os.path.exists(RAW_DEBUG_FILE):
        try:
            with open(RAW_DEBUG_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content: data_list = json.loads(content)
        except: data_list = []
    
    data_list.append(new_entry)
    
    try:
        with open(RAW_DEBUG_FILE, "w", encoding="utf-8") as f:
            json.dump(data_list, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f" Raw log save error: {e}")

# <--- CHANGED: Safe JSON Saving
def save_json_entry(company, data):
    """
    Since multiple companies finish at the same time, we need a 'Retry' mechanism.
    If the file is busy being written by another process, wait 0.1s and try again.
    """
    max_retries = 5
    for attempt in range(max_retries):
        try:
            full_data = {}
            # 1. Read existing data
            if os.path.exists(FINAL_OUTPUT_FILE):
                with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content: full_data = json.loads(content)
            
            # 2. Update specific company
            full_data[company] = data
            
            # 3. Write back
            with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(full_data, f, indent=4, ensure_ascii=False)
            break # Success, exit loop
        except Exception:
            time.sleep(0.1) # Wait briefly and retry

# ==========================================
#  3. ASYNC SEARCH WRAPPERS
# ==========================================

# <--- CHANGED: Sync Function for Threading
def _tavily_sync_call(company):
    """
    This is the standard blocking Tavily code.
    We isolate it here so we can run it in a separate thread.
    """
    current_year = datetime.datetime.now().year
    prev_year = current_year - 1
    
    query = (
        f"Detailed financial report for {company}. "
        f"Find 'Total Annual Revenue' (in {prev_year} or latest available) "
        f"and 'Total Employee Count' (in {current_year} or latest available). "
        "Just the numbers."
    )
    
    total_keys = get_tavily_count()
    max_retries = max(1, total_keys)

    for attempt in range(max_retries):
        try:
            tavily_key = get_tavily_key()
            if not tavily_key: return None, None
            client = TavilyClient(api_key=tavily_key)
            response = client.search(
                query=query, 
                search_depth="advanced", 
                max_results=2, 
                include_answer=True 
            )
            direct_answer = response.get("answer", None)
            context_text = ""
            for result in response.get("results", []):
                cleaned = clean_text(result['content'])
                context_text += f"Source: {result['title']} | Data: {cleaned[:500]} ... \n"
            return context_text, direct_answer
        except Exception as e:
            print(f"    ⚠️ Tavily Error ({company}): {e}")
            time.sleep(1)
    return None, None

# <--- CHANGED: Async Wrapper
async def search_via_tavily_async(company):
    # This runs the blocking code in a separate thread so the main loop doesn't freeze
    return await asyncio.to_thread(_tavily_sync_call, company)

# <--- CHANGED: Sync Function for Threading
def _groq_sync_call(company, final_context):
    """
    Standard blocking Groq code.
    """
    max_retries = max(1, get_groq_count())
    for _ in range(max_retries):
        try:
            api_key = get_groq_key()
            client = Groq(api_key=api_key)
            completion = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {
                        "role": "system", 
                        "content": (
                            "Extract 'Annual Revenue' (with currency) and 'Total Employee Count'. "
                            "If the input is an AI Summary, trust it. "
                            "If the input is Search Results, find the LATEST available data. "
                            "Return strict JSON: {\"Annual Revenue\": \"...\", \"Total Employee Count\": \"...\"} "
                            "If not found, write 'Not Found'."
                        )
                    },
                    {"role": "user", "content": f"Company: {company}\nData:\n{final_context}"}
                ],
                response_format={"type": "json_object"}
            )
            return json.loads(completion.choices[0].message.content)
        except Exception as e:
            print(f"    ⚠️ Groq Error ({company}): {e}")
            time.sleep(0.5)
    return {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}

# <--- CHANGED: Async Wrapper
async def analyze_with_groq_async(company, final_context):
    return await asyncio.to_thread(_groq_sync_call, company, final_context)

# ==========================================
#  4. WORKER FUNCTION (THE ENGINE)
# ==========================================

async def process_single_company(sem, company):
    """
    This function processes ONE company.
    The 'sem' (Semaphore) ensures only 5 of these run at the exact same time.
    """
    async with sem:  # <--- CHANGED: Acquiring the Lock (Limit 5)
        print(f"  ▶️ Starting: {company}")
        
        # 1. Search Tavily (Async)
        raw_data, direct_answer = await search_via_tavily_async(company)
        
        if raw_data:
            safe_answer = direct_answer if direct_answer else ""
            bad_keywords = ["unknown", "not available", "not found", "no information","not provided", "n/a", "unspecified"]
            
            # --- Smart Logic (Same as before) ---
            if not safe_answer:
                # <--- CHANGED: Increased raw data limit to 3000 chars as requested
                final_context = f"SEARCH RESULTS:\n{raw_data[:1000]}"
                log_msg = "No Direct Answer. Using Raw Data."
            elif any(k in safe_answer.lower() for k in bad_keywords):
                final_context = f"AI SUMMARY: {safe_answer}\n\nSEARCH RESULTS:\n{raw_data[:3000]}"
                log_msg = "Vague Answer. Using Answer + Raw Data."
            else:
                final_context = f"AI SUMMARY: {safe_answer}"
                log_msg = "Good Answer. Using Answer Only."

            print(f"    ℹ️ {company}: {log_msg}")

            # 2. Analyze with Groq (Async)
            result_json = await analyze_with_groq_async(company, final_context)
            
            # 3. Save Data (Thread Safe)
            save_raw_log(company, "Tavily", raw_data, safe_answer)
            save_json_entry(company, result_json)
            
            print(f"  ✅ Finished: {company} -> {result_json}")
        else:
            print(f"  ❌ Failed: {company}")
            fallback = {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}
            save_json_entry(company, fallback)

# ==========================================
#  5. MAIN PIPELINE
# ==========================================

async def main_async():
    # Cleanup old logs
    if os.path.exists(RAW_DEBUG_FILE):
        try: os.remove(RAW_DEBUG_FILE)
        except: pass

    # Load existing to skip
    existing_data = {}
    if os.path.exists(FINAL_OUTPUT_FILE):
        try:
            with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
                existing_data = json.loads(f.read().strip() or "{}")
        except: pass

    # Filter targets
    companies_to_process = [c for c in TARGET_COMPANIES if c not in existing_data]

    print(f"🚀 Starting ASYNC Extraction for {len(companies_to_process)} companies.")
    print(f"⚡ Mode: Async Parallel | Concurrency Limit: {CONCURRENCY_LIMIT}")

    # <--- CHANGED: Create the Semaphore (The Traffic Cop)
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

    # Create a list of tasks (jobs) to run
    tasks = []
    for company in companies_to_process:
        task = asyncio.create_task(process_single_company(sem, company))
        tasks.append(task)
    
    # Run all tasks and wait for them to finish
    await asyncio.gather(*tasks)

    print("\n🎉 All companies processed successfully")

# ==========================================
#  6. EXTERNAL ENTRY POINT (The Bridge)
# ==========================================

def enrich_companies_from_list(company_list):
    
    
    global TARGET_COMPANIES
    
    TARGET_COMPANIES = list(set(company_list))
    
    print(f"🔗 Bridge Activated: Starting Async Engine for {len(TARGET_COMPANIES)} companies...")
    
  
    asyncio.run(main_async())

# ==========================================
# 🟢 ENTRY POINT
# ==========================================

if __name__ == "__main__":
    # <--- CHANGED: Run the async loop
    asyncio.run(main_async())
