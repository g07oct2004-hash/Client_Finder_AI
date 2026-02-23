# import os
# import json
# import asyncio
# import pandas as pd
# from groq import AsyncGroq
# from typing import List, Dict, Any
# from dotenv import load_dotenv

# # Import your existing API key manager
# from API_rotation import get_groq_key, get_groq_count

# # --- CONFIGURATION ---
# load_dotenv()
# INPUT_FOLDER = "Unstructured_data"
# OUTPUT_CSV = "Enrichment_Required.csv"

# # --- STEP 1: AI EXTRACTION LOGIC ---
# async def extract_identity_with_groq(company_name: str, raw_text: str, sem: asyncio.Semaphore) -> Dict:
#     """
#     Uses Groq (Llama-3) to find CEO Name, First/Last Name, Domain, and LinkedIn.
#     """
    
#     # We limit text to avoid token limits, but keep enough for context
#     context_text = raw_text[:15000] 

#     prompt = f"""
#     You are a Data Extraction Expert. 
#     Analyze the following unstructured company data for '{company_name}' and extract specific identity details.
    
#     **DATA:**
#     {context_text}

#     **GOAL:**
#     Extract the following fields accurately. If a field is not found, return "Not Found".
#     1. **Official_Domain**: The main website domain (e.g., 'anavclouds.com').
#     2. **CEO_Full_Name**: The name of the current CEO or Founder.
#     3. **CEO_First_Name**: Just the first name (for email greetings).
#     4. **CEO_Last_Name**: Just the last name.
#     5. **Company URL**:Company URL or Link.
#     6. **LinkedIn_URL**: The CEO's PERSONAL LinkedIn profile URL (e.g., https://linkedin.com/in/name). 
#        - **CRITICAL**: Do NOT return a company page (like linkedin.com/company/...). 
#        - If the personal profile is NOT found, return "Not Found".

#     **STRICT OUTPUT FORMAT:**
#     Return ONLY a valid JSON object. Do not add any markdown formatting (```json).
    
#     Example Output:
#     {{
#         "Official_Domain": "example.com",
#         "CEO_Full_Name": "John Doe",
#         "CEO_First_Name": "John",
#         "CEO_Last_Name": "Doe",
#         "Company_URL":"https://www.Companyname.com/"
#         "LinkedIn_URL": "[linkedin.com/in/johndoe](https://linkedin.com/in/johndoe)"
#     }}
#     """

#     async with sem:
#         total_keys = get_groq_count()
#         max_retries = max(1, total_keys)

#         for attempt in range(max_retries):
#             try:
#                 # Get a fresh key for every request
#                 api_key = get_groq_key()
#                 client = AsyncGroq(api_key=api_key)

#                 completion = await client.chat.completions.create(
#                     messages=[
#                         {"role": "system", "content": "You are a precise JSON extractor."},
#                         {"role": "user", "content": prompt}
#                     ],
#                     model="llama-3.3-70b-versatile",
#                     temperature=0.1, # Low temperature for factual accuracy
#                     response_format={"type": "json_object"} # Force JSON mode
#                 )

#                 response_content = completion.choices[0].message.content
#                 return json.loads(response_content)

#             except Exception as e:
#                 print(f" Groq Attempt {attempt+1} Failed for {company_name}: {e}")
#                 await asyncio.sleep(1)

#         # If all retries fail, return empty placeholders
#         return {
#             "Official_Domain": "Not Found",
#             "CEO_Full_Name": "Not Found",
#             "CEO_First_Name": "",
#             "CEO_Last_Name": "",
#             "Company_URL": "",
#             "LinkedIn_URL": ""
#         }

# # --- STEP 2: FILE PROCESSING ---
# async def process_company_file(file_path: str, sem: asyncio.Semaphore) -> Dict:
#     """
#     Reads a single JSON file and orchestrates the extraction.
#     """
#     try:
#         with open(file_path, "r", encoding="utf-8") as f:
#             data = json.load(f)

#         # Handle different JSON structures (support both old and new formats)
#         company_name = data.get("meta", {}).get("company_name", "Unknown Company")
        
#         # Combine all relevant text for the AI to read
#         # We prefer the specific "company_identity" section if it exists (from your new update)
#         identity_section = json.dumps(data.get("company_identity", {}))
#         financial_section = json.dumps(data.get("financial_intelligence", []))
        
#         raw_text = f"IDENTITY_DATA: {identity_section}\n\nFINANCIAL_DATA: {financial_section}"

#         print(f" Enriching: {company_name}...")
        
#         # Call AI
#         extracted_data = await extract_identity_with_groq(company_name, raw_text, sem)
        
#         # Merge Company Name into the result
#         extracted_data["Company_Name"] = company_name
        
#         # Add the EMPTY Email column for the user to fill
#         extracted_data["Email_ID"] = "" 
        
#         return extracted_data

#     except Exception as e:
#         print(f" Error processing file {file_path}: {e}")
#         return None

# # --- STEP 3: MAIN EXECUTION ---
# async def run_data_enrichment():
#     """
#     Main function to run the enrichment workflow.
#     """
#     if not os.path.exists(INPUT_FOLDER):
#         print(f" Folder '{INPUT_FOLDER}' not found. Run Deep Research first.")
#         return "Failed: No Data Found"

#     # 1. List all JSON files
#     files = [
#         os.path.join(INPUT_FOLDER, f) 
#         for f in os.listdir(INPUT_FOLDER) 
#         if f.endswith(".json")
#     ]

#     if not files:
#         print(" No JSON reports found to enrich.")
#         return "Failed: No Reports"

#     print(f"\n Starting Enrichment for {len(files)} companies...")
    
#     # 2. Setup Concurrency (Process 5 files at a time)
#     sem = asyncio.Semaphore(5)
#     tasks = [process_company_file(f, sem) for f in files]

#     # 3. Run all tasks
#     results = await asyncio.gather(*tasks)
    
#     # Filter out any failed results (None)
#     valid_results = [r for r in results if r]

#     if not valid_results:
#         print(" No data extracted.")
#         return "Failed"

#     # 4. Create DataFrame and Reorder Columns
#     df = pd.DataFrame(valid_results)
    
#     # Desired Column Order for the User CSV
#     cols = [
#         "Company_Name", 
#         "Official_Domain",
#         "Company_URL", 
#         "CEO_Full_Name", 
#         "CEO_First_Name", 
#         "CEO_Last_Name", 
#         "LinkedIn_URL", 
#         "Email_ID"  # This will be empty, waiting for user input
#     ]
    
#     # Ensure all columns exist (in case AI missed one)
#     for c in cols:
#         if c not in df.columns:
#             df[c] = ""
            
#     df = df[cols] # Reorder

#     # 5. Save to CSV
#     df.to_csv(OUTPUT_CSV, index=False)
#     print(f"\n Enrichment Complete! CSV saved to: {OUTPUT_CSV}")
#     return OUTPUT_CSV

# if __name__ == "__main__":
#     asyncio.run(run_data_enrichment())




import os
import json
import asyncio
import pandas as pd
from groq import AsyncGroq
from typing import List, Dict, Any
from dotenv import load_dotenv
import gspread
import urllib.parse
import random
from google.oauth2.service_account import Credentials
# Import your existing API key manager
from API_rotation import get_groq_key, get_groq_count
 
# --- CONFIGURATION ---
load_dotenv()
INPUT_FOLDER = "Unstructured_data"
OUTPUT_CSV = "Enrichment_Required.csv"
 
def extract_urls_only(data):
    urls = []
    for item in data.get("official_website_data", []):
        url = item.get("url")
        if url:
            urls.append(url)
    return urls
 
 

# Comprehensive blocklist to prevent Apollo credit waste
BLOCKED_DOMAINS = [
    "linkedin.com", "facebook.com", "twitter.com", "instagram.com", "youtube.com",
    "zoominfo.com", "crunchbase.com", "propublica.org", "wikipedia.org",
    "bloomberg.com", "pitchbook.com", "glassdoor.com", "indeed.com",
    "apollo.io", "dnb.com", "g2.com", "capterra.com", "owler.com",
    "yellowpages.com", "yelp.com", "trustpilot.com", "guidestar.org", "angel.co", "wellfound.com"
]

def is_valid_company_url(url: str) -> bool:
    """Validates if a URL is an actual company domain and not a directory."""
    if not url or str(url).strip().lower() in ["not found", "", "null", "none"]:
        return False
        
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            
        domain = urllib.parse.urlparse(url).netloc.lower()
        
        for blocked in BLOCKED_DOMAINS:
            if blocked in domain:
                return False
                
        if "." not in domain:
            return False
            
        return True
    except Exception:
        return False
    

def get_base_homepage_url(url: str) -> str:
    """
    Extracts strictly the base homepage URL (e.g., https://www.example.com) 
    and removes any subpages (/about-us, /careers) or queries.
    """
    if not url or pd.isna(url) or str(url).strip().lower() in ["nan", "none", "", "not found"]:
        return ""
        
    url = str(url).strip()
    
    # Ensure scheme exists so urlparse works correctly
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
        
    try:
        parsed_url = urllib.parse.urlparse(url)
        # Rebuild the URL using only the scheme (https) and the main domain (netloc)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        return base_url
    except Exception:
        return url

# # --- STEP 1: AI EXTRACTION LOGIC ---
# async def extract_identity_with_groq(company_name: str, raw_text: str, urls: List[str], sem: asyncio.Semaphore) -> Dict:
#     """
#     Uses Groq (Llama-3) to find CEO Name, First/Last Name, Domain, and LinkedIn.
#     """
   
#     # We limit text to avoid token limits, but keep enough for context
#     context_text = raw_text[:15000]
 
#     prompt = f"""
#     You are a Data Extraction Expert.
#     Analyze the following unstructured company data for '{company_name}' and extract specific identity details.
#     URL LIST:
#     {urls}
#     **DATA:**
#     {context_text}
 
#     **GOAL:**
#     Extract the following fields accurately. If a field is not found, return "Not Found".
#     1. **Official_Domain**: The main website domain (e.g., 'anavclouds.com').
#     2. **CEO_Full_Name**: The name of the current CEO or Founder.
#     3. **CEO_First_Name**: Just the first name (for email greetings).
#     4. **CEO_Last_Name**: Just the last name.
#     5. **Company URL**:Company URL or Link.
#     6. **LinkedIn_URL**: The CEO's PERSONAL LinkedIn profile URL (e.g., https://linkedin.com/in/name).
#        - **CRITICAL**: Do NOT return a company page (like linkedin.com/company/...).
#        - If the personal profile is NOT found, return "Not Found".
 
#     **STRICT OUTPUT FORMAT:**
#     Return ONLY a valid JSON object. Do not add any markdown formatting (```json).
   
#     Example Output:
#     {{
#         "Official_Domain": "example.com",
#         "CEO_Full_Name": "John Doe",
#         "CEO_First_Name": "John",
#         "CEO_Last_Name": "Doe",
#         "Company_URL":"https://www.Companyname.com/"
#         "LinkedIn_URL": "[linkedin.com/in/johndoe](https://linkedin.com/in/johndoe)"
#     }}
#     """
 
#     async with sem:
#         total_keys = get_groq_count()
#         max_retries = max(1, total_keys)
 
#         for attempt in range(max_retries):
#             try:
#                 # Get a fresh key for every request
#                 # api_key = get_groq_key()
#                 api_key = get_groq_key(delay=5)
#                 client = AsyncGroq(api_key=api_key)
 
#                 completion = await client.chat.completions.create(
#                     messages=[
#                         {"role": "system", "content": "You are a precise JSON extractor."},
#                         {"role": "user", "content": prompt}
#                     ],
#                     model="llama-3.3-70b-versatile",
#                     temperature=0.1, # Low temperature for factual accuracy
#                     response_format={"type": "json_object"} # Force JSON mode
#                 )
 
#                 response_content = completion.choices[0].message.content
#                 return json.loads(response_content)
 
#             except Exception as e:
#                 print(f" Groq Attempt {attempt+1} Failed for {company_name}: {e}")
#                 await asyncio.sleep(1)
 
#         # If all retries fail, return empty placeholders
#         return {
#             "Official_Domain": "Not Found",
#             "CEO_Full_Name": "Not Found",
#             "CEO_First_Name": "",
#             "CEO_Last_Name": "",
#             "Company_URL": "",
#             "LinkedIn_URL": ""
#         }
 
# --- STEP 1: AI EXTRACTION LOGIC ---
async def extract_identity_with_groq(company_name: str, raw_text: str, urls: List[str], sem: asyncio.Semaphore) -> Dict:
    """
    Uses Groq (Llama-3) to find Official Domain and Company URL only.
    Optimized to save tokens and reduce latency.
    """
    # We limit text to avoid token limits, but keep enough for context
    context_text = raw_text[:15000]

    prompt = f"""
    You are a Data Extraction Expert.
    Analyze the following unstructured company data for '{company_name}'.
    URL LIST:
    {urls}
    **DATA:**
    {context_text}

    **GOAL:**
    Extract the following fields accurately. If a field is not found, return "".
    1. **Official_Domain**: The main website domain (e.g., 'anavclouds.com'). 
       - CRITICAL: MUST NOT be a directory or social site.
    2. **Company_URL**: Company URL or Link. 
       - STRICT RULE: DO NOT return URLs from linkedin.com, propublica.org, crunchbase.com, zoominfo.com, etc. If only these exist, return "".

    **STRICT OUTPUT FORMAT:**
    Return ONLY a valid JSON object. Do not add any markdown formatting (```json).
    
    Example Output:
    {{
        "Official_Domain": "example.com",
        "Company_URL": "[https://www.example.com](https://www.example.com)"
    }}
    """

    async with sem:
        await asyncio.sleep(random.uniform(1, 3))
        total_keys = get_groq_count()
        max_retries = max(1, total_keys)

        for attempt in range(max_retries):
            try:
                # Get a fresh key for every request
                api_key = get_groq_key(delay=3)
                client = AsyncGroq(api_key=api_key)

                completion = await client.chat.completions.create(
                    messages=[
                        {"role": "system", "content": "You are a precise JSON extractor."},
                        {"role": "user", "content": prompt}
                    ],
                    model="llama-3.3-70b-versatile",
                    temperature=0.1, # Low temperature for factual accuracy
                    max_tokens=150,
                    response_format={"type": "json_object"} # Force JSON mode
                )

                response_content = completion.choices[0].message.content
                return json.loads(response_content)

            except Exception as e:
                print(f" Groq Attempt {attempt+1} Failed for {company_name}: {e}")
                await asyncio.sleep(1)

        # If all retries fail, return empty placeholders
        return {
            "Official_Domain": "",
            "Company_URL": ""
        }
# --- STEP 2: FILE PROCESSING ---
 
 
async def process_company_file(file_path: str, sem: asyncio.Semaphore) -> Dict:
    """
    Reads a single JSON file and orchestrates the extraction.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
 
        # Handle different JSON structures (support both old and new formats)
        company_name = data.get("meta", {}).get("company_name", "Unknown Company")
       
        # Combine all relevant text for the AI to read
        # We prefer the specific "company_identity" section if it exists (from your new update)
        identity_section = json.dumps(data.get("market_updates", {}))
        financial_section = json.dumps(data.get("financial_intelligence", []))
        website_section = json.dumps(data.get("official_website_data", {}))
        urls = extract_urls_only(data)
 
        raw_text = f"IDENTITY_DATA: {identity_section}\n\nFINANCIAL_DATA: {financial_section}\n\nWEBSITE_DATA: {website_section}"
 
        print(f" Enriching: {company_name}...")
       
        # Call AI
        extracted_data = await extract_identity_with_groq(company_name, raw_text,urls,  sem)
       
        # ---------------------------------------------------------
        # 👇 AUTO-CLEAN URLs HERE BEFORE SAVING 👇
        # ---------------------------------------------------------
        if "Company_URL" in extracted_data:
            extracted_data["Company_URL"] = get_base_homepage_url(extracted_data["Company_URL"])
        # ---------------------------------------------------------
        
        # Merge Company Name into the result
        extracted_data["Company_Name"] = company_name
       
        # Add the EMPTY Email column for the user to fill
        extracted_data["Email_ID"] = ""
       
        return extracted_data
 
    except Exception as e:
        print(f" Error processing file {file_path}: {e}")
        return None
 


#-----------------------------
# --- GOOGLE SHEET SYNC FUNCTION (Add this after imports) ---
def connect_to_sheet():
    """Connects to Google Sheets using the GOOGLE_SERVICE_ACCOUNT_JSON from .env"""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    service_account_info = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    
    if not service_account_info:
        print("❌ Error: GOOGLE_SERVICE_ACCOUNT_JSON not found in .env")
        return None

    try:
        creds_dict = json.loads(service_account_info)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return None

async def sync_urls_to_sheet(extracted_data_list):
    """
    Updates the 'company_profile_website' column by matching 
    AI data with the 'meta_company_name' in Google Sheet.
    """
    print("\n🔄 Syncing URLs to Google Sheet ('Email_tool')...")
    gc = connect_to_sheet()
    if not gc: return

    try:
        # Opening 'Email_tool' sheet
        sh = gc.open("Email_tool") 
        worksheet = sh.sheet1
        
        # 1. Row Mapping strictly based on 'meta_company_name'
        sheet_records = worksheet.get_all_records()
        company_row_map = {}
        for idx, row in enumerate(sheet_records):
            # Strict matching using meta_company_name
            m_name = str(row.get("meta_company_name", "")).strip().lower()
            if m_name:
                company_row_map[m_name] = idx + 2 # +2 for header and 0-index offset

        # 2. Find/Create 'company_profile_website' column
        headers = worksheet.row_values(1)
        target_col = "company_profile_website"
        
        if target_col not in headers:
            worksheet.update_cell(1, len(headers) + 1, target_col)
            col_idx = len(headers) + 1
        else:
            col_idx = headers.index(target_col) + 1

        # 3. Update Loop
        count = 0
        for item in extracted_data_list:
            # AI extracted name
            comp_name = item.get("Company_Name", "").strip().lower()
            new_url = item.get("Company_URL", "") or item.get("Official_Domain", "")
            
            # Match strictly with meta_company_name mapping
            if comp_name in company_row_map and new_url and new_url != "Not Found":
                row_num = company_row_map[comp_name]
                try:
                    worksheet.update_cell(row_num, col_idx, new_url)
                    count += 1
                except Exception as e:
                    print(f"   ⚠️ Error updating {comp_name}: {e}")
                
        print(f"✅ Success! Updated {count} rows based on meta_company_name match.")

    except Exception as e:
        print(f"⚠️ Sheet Update Failed: {e}")
#-----------------------------
# --- STEP 3: MAIN EXECUTION ---
async def run_data_enrichment():
    """
    Main function to run the enrichment workflow.
    """
    if not os.path.exists(INPUT_FOLDER):
        print(f" Folder '{INPUT_FOLDER}' not found. Run Deep Research first.")
        return "Failed: No Data Found"
 
    # 1. List all JSON files
    files = [
        os.path.join(INPUT_FOLDER, f)
        for f in os.listdir(INPUT_FOLDER)
        if f.endswith(".json")
    ]
 
    if not files:
        print(" No JSON reports found to enrich.")
        return "Failed: No Reports"
 
    print(f"\n Starting Enrichment for {len(files)} companies...")
   
    # 2. Setup Concurrency (Process 5 files at a time)
    sem = asyncio.Semaphore(5)
    tasks = [process_company_file(f, sem) for f in files]
 
    # 3. Run all tasks
    results = await asyncio.gather(*tasks)
   
    # Filter out any failed results (None)
    valid_results = [r for r in results if r]
 
    if not valid_results:
        print(" No data extracted.")
        return "Failed"
    
    await sync_urls_to_sheet(valid_results)
    # 4. Create DataFrame and Reorder Columns
    df = pd.DataFrame(valid_results)
   
    # Desired Column Order for the User CSV
    # cols = [
    #     "Company_Name",
    #     "Official_Domain",
    #     "Company_URL",
    #     "CEO_Full_Name",
    #     "CEO_First_Name",
    #     "CEO_Last_Name",
    #     "LinkedIn_URL",
    #     "Email_ID"  # This will be empty, waiting for user input
    # ]
    cols = [
        "Company_Name",
        "Official_Domain",
        "Company_URL"
        
    ]
   
    # Ensure all columns exist (in case AI missed one)
    for c in cols:
        if c not in df.columns:
            df[c] = ""
           
    df = df[cols] # Reorder
 
    # 5. Save to CSV
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n Enrichment Complete! CSV saved to: {OUTPUT_CSV}")
    return OUTPUT_CSV
 
if __name__ == "__main__":
    asyncio.run(run_data_enrichment())