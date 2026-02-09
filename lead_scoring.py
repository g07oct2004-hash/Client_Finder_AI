# import time
# import os
# import sys
# import asyncio
# import pandas as pd
# from dotenv import load_dotenv
# from groq import AsyncGroq
# import gspread
# from google.oauth2.service_account import Credentials
# import json
# import funding 
# from API_rotation import get_groq_key,get_groq_count
# # --- 1. SETUP & CONFIGURATION ---
# load_dotenv()

# def get_api_keys(prefix):
#     keys = []
#     i = 1
#     while True:
#         key = os.getenv(f"{prefix}_{i}")
#         if key:
#             keys.append(key)
#             i += 1
#         else:
#             break
#     if not keys and os.getenv(prefix):
#         keys.append(os.getenv(prefix))
#     return keys

# GROQ_KEYS = get_api_keys("GROQ_API_KEY")

# if not GROQ_KEYS:
#     if os.getenv("GROQ_API_KEY"):
#         GROQ_KEYS = [os.getenv("GROQ_API_KEY")]
#     else:
#         raise ValueError("[CRITICAL ERROR] No GROQ_API_KEYs found in .env file")

# try:
#     from upload_to_sheets import GOOGLE_SHEET_NAME
# except ImportError:
#     GOOGLE_SHEET_NAME = "Email_tool" 
#     print(f"[WARNING] Could not import sheet name. Using default: {GOOGLE_SHEET_NAME}")

# # --- 2. GOOGLE SHEETS CONNECTION ---

# def connect_to_sheet():
#     scopes = [
#         "https://www.googleapis.com/auth/spreadsheets",
#         "https://www.googleapis.com/auth/drive"
#     ]
#     try:
#         service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
#         creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
#         gc = gspread.authorize(creds)
#         return gc
#     except Exception as e:
#         print(f"Connection Failed: {e}")
#         sys.exit(1)

# # --- 3. THE AI BRAIN (Strategic Analysis) ---

# # [FIX] Semaphore is no longer global. It is now passed as an argument.

# async def generate_smart_summary(row_data, sem):
#     """
#     Reads Row Data + FUNDING STATUS and generates a Professional Analyst Summary.
#     """
#     # [FIX] Enforce concurrency limit using the passed semaphore
#     async with sem:
#         company = row_data.get("company_profile_company_name", row_data.get("Company", "Unknown"))
#         industry = row_data.get("company_profile_industry", "Unknown")
#         revenue = row_data.get("financials_estimated_revenue_usd", "Unknown")
#         employees = row_data.get("financials_employees", "Unknown")
#         funding_status = row_data.get("Funding Status", "Unknown") 

#         score = row_data.get("lead_scoring_lead_score") or row_data.get("Lead Score") or "0"
#         breakout = row_data.get("lead_scoring_rank_breakout") or row_data.get("Rank (Breakout)") or "No details"
#         news_data = row_data.get("news", "")
        
#         prompt = f"""
# Act as a Senior Business Analyst for a specialized Agency that provides Salesforce & AI Automation services.
 
# # We specialize in:
# # - Salesforce Consulting, Implementation, Custom Development & Integrations
# # - Salesforce Clouds (Sales, Service, Marketing, Industry, Data Cloud)
# # - AI/ML, Generative AI, NLP & Intelligent Automation ,Agentic AI
# # - Data Analytics, BI & Predictive Insights
# # - CRM + ERP + API integrations (SAP, Oracle, NetSuite, Zoho)
# # - Ongoing Managed Services & Long-term Partnerships

# **CONTEXT:**
# We are analyzing leads to find the "Perfect Fit" for our agency.
# - **Perfect Fit:** Mid-Sized, Funded ($M), or Growing SMBs. (These need Speed & Efficiency).
# - **Bad Fit / Low Priority:** Public Listed Companies (Too slow, too much red tape).
# - first check the Bootstrep or Ravenue is low then the verdict is High Potential 
# # Write a **2-sentence sales intelligence summary** that clearly explains:
# # 1. **Why THIS company is a good fit for OUR services**
# # 2. **Why NOW is the right time for us to reach out**

# **LEAD DATA:**
# - Company: {company}
# - **Funding Status: {funding_status}** (CRITICAL SIGNAL)
# - Scoring Breakdown: {breakout}
# - Size: {employees} Employees | Revenue: {revenue}
# - Score: {score}/100
# - Recent Signals / News: {news_data[:300]}

# **YOUR TASK:**
# Write a strict, logic-based analysis for our Marketing Team explaining WHY this company got this score and HOW they fit our agency model.


# **LOGIC RULES:**
# 1. **If Funding is High ($M/$B) OR "Series A/B/C":** - Verdict: "Prime Target (Rapid Growth)"
#    - Logic: Explain that fresh funding means they have budget but lack internal infrastructure. They need our "Rapid Deployment" services.
   
# 2. **If Bootstrapped AND Revenue is Low:** - Verdict: "High Potential SMB (Efficiency Focus)"
#    - Logic: Explain that they are cost-sensitive. They need our "AI Automation" to scale revenue without hiring more staff.

# 3. **If Public Listed:** - Verdict: "Low Priority Enterprise"
#    - Logic: Explain that despite high revenue, their Public status implies slow decision-making. Likely only good for "Staff Augmentation", not full projects.

# **STRICT OUTPUT FORMAT:**
# Verdict: [Insert Classification]
# Analysis: [2 sentences purely explaining the business logic.why this company is best for us. No sales fluff.]

# """ # (Prompt remains exactly as you had it)

#         # for index, api_key in enumerate(GROQ_KEYS):
#         #     try:
#         #         client = AsyncGroq(api_key=api_key)
#         #         chat_completion = await client.chat.completions.create(
#         #             messages=[{"role": "user", "content": prompt}],
#         #             model="llama-3.3-70b-versatile", 
#         #             temperature=0.7,
                    
#         #         )
#         #         return chat_completion.choices[0].message.content.strip()
#         #     except Exception as e:
#         #         print(f" Groq Key {index+1} Failed for {company}: {e}")
#         #         if index < len(GROQ_KEYS) - 1:
#         #             await asyncio.sleep(1)
#         #         else:
#         #             return "Analysis Failed"
        
#         # 1. Get the total count of keys from the manager
#         total_keys = get_groq_count()
        
#         # Safety: Ensure we try at least once even if count returns 0 (unlikely)
#         max_retries = max(1, total_keys)
#         for attempt in range(max_retries):
#             try:
#                 # 1. Get a fresh key from the manager
#                 api_key = get_groq_key()
                
#                 # 2. Create the client with this specific key
#                 client = AsyncGroq(api_key=api_key)
                
#                 chat_completion = await client.chat.completions.create(
#                     messages=[{"role": "user", "content": prompt}],
#                     model="llama-3.3-70b-versatile", 
#                     temperature=0.7,
#                 )
#                 return chat_completion.choices[0].message.content.strip()
                
#             except Exception as e:
#                 print(f" Groq Attempt {attempt+1} Failed for {company}: {e}")
#                 # Wait briefly before retrying with a new key
#                 await asyncio.sleep(1)

#         return "Analysis Failed"

# # --- 4. MAIN EXECUTION ---

# # [FIX] Added 'sem' to parameters
# async def process_row_task(index, row, worksheet, output_col_idx, sem):
#     company_name = row.get("company_profile_company_name", row.get("Company", "Unknown"))

#     existing_summary = str(row.get("AI Strategic Summary", "")).strip()
#     is_failed_previous_run = any(x in existing_summary.lower() for x in ["analysis failed", "error", "failed to update"])
    
#     if len(existing_summary) > 10 and not is_failed_previous_run:
#         return 0 
    
#     score_val = (row.get("lead_scoring_lead_score") or row.get("Lead Score"))
#     if not score_val:
#         return 0

#     print(f" Analyzing {company_name}...")
    
#     # [FIX] Pass semaphore to the summary generator
#     summary = await generate_smart_summary(row, sem)

#     try:
#         worksheet.update_cell(index + 2, output_col_idx, summary)
#         print(f" Success! Updated {company_name}")
#         return 1
#     except Exception as e:
#         print(f" Failed to update {company_name}: {e}")
#         return 0

# async def process_sheet_smartly_async():
#     # [FIX] Define Semaphore here to bind it to the correct, active event loop
#     sem = asyncio.Semaphore(3) 
    
#     print("\n STARTING PHASE 1: FUNDING ANALYSIS...")
#     try:
#         await funding.main_async() 
#     except Exception as e:
#         print(f" Funding Script Error: {e}")
    
#     print("\n PHASE 1 COMPLETE.")
#     print(" Cooling down for 30 seconds...")
#     await asyncio.sleep(30)

#     gc = connect_to_sheet()
#     try:
#         sh = gc.open(GOOGLE_SHEET_NAME)
#         worksheet = sh.sheet1 
#     except Exception as e:
#         print(f" Could not open sheet: {e}")
#         return

#     raw_data = worksheet.get_all_values()
#     if not raw_data: return

#     headers = raw_data[0]
#     rows = raw_data[1:]
#     df = pd.DataFrame(rows, columns=headers)

#     output_col = "AI Strategic Summary"
#     try:
#         col_idx = headers.index(output_col) + 1
#     except ValueError:
#         worksheet.add_cols(1)
#         col_idx = len(headers) + 1
#         worksheet.update_cell(1, col_idx, output_col)

#     # [FIX] Pass the created semaphore to each task
#     tasks = []
#     for index, row in df.iterrows():
#         task = process_row_task(index, row, worksheet, col_idx, sem)
#         tasks.append(task)

#     results = await asyncio.gather(*tasks)
#     total_updates = sum(results)
#     print(f" Finished! Total Updated: {total_updates} companies.")

# def run_ai_strategic_layer():
#     asyncio.run(process_sheet_smartly_async())

# if __name__ == "__main__":
#     run_ai_strategic_layer()



















import time
import os
import sys
import asyncio
import pandas as pd
from dotenv import load_dotenv
from groq import AsyncGroq
import gspread
from google.oauth2.service_account import Credentials
import json
from API_rotation import get_groq_key, get_groq_count

# --- 1. SETUP & CONFIGURATION ---
load_dotenv()

def get_api_keys(prefix):
    keys = []
    i = 1
    while True:
        key = os.getenv(f"{prefix}_{i}")
        if key:
            keys.append(key)
            i += 1
        else:
            break
    if not keys and os.getenv(prefix):
        keys.append(os.getenv(prefix))
    return keys

GROQ_KEYS = get_api_keys("GROQ_API_KEY")

if not GROQ_KEYS:
    if os.getenv("GROQ_API_KEY"):
        GROQ_KEYS = [os.getenv("GROQ_API_KEY")]
    else:
        raise ValueError("[CRITICAL ERROR] No GROQ_API_KEYs found in .env file")

try:
    from upload_to_sheets import GOOGLE_SHEET_NAME
except ImportError:
    GOOGLE_SHEET_NAME = "Email_tool" 
    print(f"[WARNING] Could not import sheet name. Using default: {GOOGLE_SHEET_NAME}")

# --- 2. GOOGLE SHEETS CONNECTION ---

def connect_to_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    try:
        service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        gc = gspread.authorize(creds)
        return gc
    except Exception as e:
        print(f"Connection Failed: {e}")
        sys.exit(1)

# --- 3. THE AI BRAIN (Strategic Analysis) ---

async def generate_smart_summary(row_data, sem):
    """
    Reads ONLY the requested columns (Score, Breakout, Financials, Market Updates)
    and generates a Professional Analyst Summary.
    """
    # Enforce concurrency limit using the passed semaphore
    async with sem:
        # 1. Identity
        company = row_data.get("company_profile_company_name", row_data.get("Company", "Unknown"))
        
        # 2. Score & Breakout
        score = row_data.get("lead_scoring_lead_score") or row_data.get("Lead Score") or "0"
        breakout = row_data.get("lead_scoring_rank_breakout") or row_data.get("Rank (Breakout)") or "No details"
        
        # 3. Raw Intelligence Data (Used instead of specific columns)
        # Limiting to 3000 chars to save tokens while keeping context
        financial_intel = str(row_data.get("financial_intelligence", "No Data"))[:3000]
        market_updates = str(row_data.get("market_updates", "No Data"))[:3000]
        
        # --- PROMPT ---
#         prompt = f"""
# Act as a Senior Business Analyst for a specialized Agency that provides Salesforce & AI Automation services.
 
# # We specialize in:
# # - Salesforce Consulting, Implementation, Custom Development & Integrations
# # - Salesforce Clouds (Sales, Service, Marketing, Industry, Data Cloud)
# # - AI/ML, Generative AI, NLP & Intelligent Automation ,Agentic AI
# # - Data Analytics, BI & Predictive Insights
# # - CRM + ERP + API integrations (SAP, Oracle, NetSuite, Zoho)
# # - Ongoing Managed Services & Long-term Partnerships

# **CONTEXT:**
# We are analyzing leads to find the "Perfect Fit" for our agency.
# - **Perfect Fit:** Mid-Sized, Funded ($M), or Growing SMBs. (These need Speed & Efficiency).
# - **Bad Fit / Low Priority:** Public Listed Companies (Too slow, too much red tape).
# - first check the Bootstrep or Ravenue is low then the verdict is High Potential 

# # Write a **2-sentence sales intelligence summary** that clearly explains:
# # 1. **Why THIS company is a good fit for OUR services**
# # 2. **Why NOW is the right time for us to reach out**

# **LEAD DATA:**
# - Company: {company}
# - Score: {score}/100
# - Scoring Breakdown: {breakout}

# **FINANCIAL INTELLIGENCE (Raw Data):**
# {financial_intel}

# **MARKET UPDATES (Raw Data):**
# {market_updates}

# **YOUR TASK:**
# Write a strict, logic-based analysis for our Marketing Team explaining WHY this company got this score and HOW they fit our agency model.
# Use the Financial Intelligence text to infer their Funding Status, Revenue, and Size for your logic.

# **LOGIC RULES:**
# 1. **If Funding is High ($M/$B) OR "Series A/B/C":** - Verdict: "Prime Target (Rapid Growth)"
#    - Logic: Explain that fresh funding means they have budget but lack internal infrastructure. They need our "Rapid Deployment" services.
   
# 2. **If Bootstrapped AND Revenue is Low:** - Verdict: "High Potential SMB (Efficiency Focus)"
#    - Logic: Explain that they are cost-sensitive. They need our "AI Automation" to scale revenue without hiring more staff.

# 3. **If Public Listed:** - Verdict: "Low Priority Enterprise"
#    - Logic: Explain that despite high revenue, their Public status implies slow decision-making. Likely only good for "Staff Augmentation", not full projects.

# **STRICT OUTPUT FORMAT:**
# Verdict: [Insert Classification]
# Analysis: [2 sentences purely explaining the business logic.why this company is best for us. No sales fluff.]
# """ 
        
        prompt = f"""
Act as a Senior Market Research Analyst.

**DATA SOURCE:**
- **Company:** {company}
- **Financial Intelligence:** {financial_intel}
- **Market News:** {market_updates}


**YOUR TASK:**
Write a **dense, 3-4 line Company Snapshot** that covers EXACTLY these 4 points:
1. **Core Identity:** What does the company do? What are their main services/products?
2. **Industry:** What industry do they operate in?
3. **Financials:** Mention their Funding Status, Revenue, and Employee size (infer from data).
4. **Latest News:** What is the most recent significant news or market signal?

**STRICT OUTPUT FORMAT:**
[Provide a single paragraph. Start by defining the Company & Services. Then state the Financials (Revenue/Funding). End with the Latest News. Do NOT use bullet points.]
"""
        # API Rotation Logic
        total_keys = get_groq_count()
        max_retries = max(1, total_keys)
        
        for attempt in range(max_retries):
            try:
                api_key = get_groq_key()
                client = AsyncGroq(api_key=api_key)
                
                chat_completion = await client.chat.completions.create(
                    messages=[{"role": "user", "content": prompt}],
                    model="llama-3.3-70b-versatile", 
                    temperature=0.7,
                )
                return chat_completion.choices[0].message.content.strip()
                
            except Exception as e:
                print(f" Groq Attempt {attempt+1} Failed for {company}: {e}")
                await asyncio.sleep(1)

        return "Analysis Failed"

# --- 4. MAIN EXECUTION ---

async def process_row_task(index, row, worksheet, output_col_idx, sem):
    company_name = row.get("company_profile_company_name", row.get("Company", "Unknown"))

    existing_summary = str(row.get("AI Strategic Summary", "")).strip()
    is_failed_previous_run = any(x in existing_summary.lower() for x in ["analysis failed", "error", "failed to update"])
    
    # Optional: Skip if already done
    if len(existing_summary) > 10 and not is_failed_previous_run:
        return 0 
    
    # Check if Lead Score exists
    score_val = (row.get("lead_scoring_lead_score") or row.get("Lead Score"))
    if not score_val:
        return 0

    print(f" Analyzing {company_name}...")
    
    # Pass semaphore to the summary generator
    summary = await generate_smart_summary(row, sem)

    try:
        worksheet.update_cell(index + 2, output_col_idx, summary)
        print(f" Success! Updated {company_name}")
        return 1
    except Exception as e:
        print(f" Failed to update {company_name}: {e}")
        return 0

async def process_sheet_smartly_async():
    # Define Semaphore here
    sem = asyncio.Semaphore(3) 
    
    # Note: Funding Script Removed

    gc = connect_to_sheet()
    try:
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.sheet1 
    except Exception as e:
        print(f" Could not open sheet: {e}")
        return

    raw_data = worksheet.get_all_values()
    if not raw_data: return

    headers = raw_data[0]
    rows = raw_data[1:]
    df = pd.DataFrame(rows, columns=headers)

    output_col = "AI Strategic Summary"
    try:
        col_idx = headers.index(output_col) + 1
    except ValueError:
        worksheet.add_cols(1)
        col_idx = len(headers) + 1
        worksheet.update_cell(1, col_idx, output_col)

    tasks = []
    for index, row in df.iterrows():
        task = process_row_task(index, row, worksheet, col_idx, sem)
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    total_updates = sum(results)
    print(f" Finished! Total Updated: {total_updates} companies.")

def run_ai_strategic_layer():
    asyncio.run(process_sheet_smartly_async())

if __name__ == "__main__":
    run_ai_strategic_layer()