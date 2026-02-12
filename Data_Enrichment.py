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
 
 
# --- STEP 1: AI EXTRACTION LOGIC ---
async def extract_identity_with_groq(company_name: str, raw_text: str, urls: List[str], sem: asyncio.Semaphore) -> Dict:
    """
    Uses Groq (Llama-3) to find CEO Name, First/Last Name, Domain, and LinkedIn.
    """
   
    # We limit text to avoid token limits, but keep enough for context
    context_text = raw_text[:15000]
 
    prompt = f"""
    You are a Data Extraction Expert.
    Analyze the following unstructured company data for '{company_name}' and extract specific identity details.
    URL LIST:
    {urls}
    **DATA:**
    {context_text}
 
    **GOAL:**
    Extract the following fields accurately. If a field is not found, return "Not Found".
    1. **Official_Domain**: The main website domain (e.g., 'anavclouds.com').
    2. **CEO_Full_Name**: The name of the current CEO or Founder.
    3. **CEO_First_Name**: Just the first name (for email greetings).
    4. **CEO_Last_Name**: Just the last name.
    5. **Company URL**:Company URL or Link.
    6. **LinkedIn_URL**: The CEO's PERSONAL LinkedIn profile URL (e.g., https://linkedin.com/in/name).
       - **CRITICAL**: Do NOT return a company page (like linkedin.com/company/...).
       - If the personal profile is NOT found, return "Not Found".
 
    **STRICT OUTPUT FORMAT:**
    Return ONLY a valid JSON object. Do not add any markdown formatting (```json).
   
    Example Output:
    {{
        "Official_Domain": "example.com",
        "CEO_Full_Name": "John Doe",
        "CEO_First_Name": "John",
        "CEO_Last_Name": "Doe",
        "Company_URL":"https://www.Companyname.com/"
        "LinkedIn_URL": "[linkedin.com/in/johndoe](https://linkedin.com/in/johndoe)"
    }}
    """
 
    async with sem:
        total_keys = get_groq_count()
        max_retries = max(1, total_keys)
 
        for attempt in range(max_retries):
            try:
                # Get a fresh key for every request
                api_key = get_groq_key()
                client = AsyncGroq(api_key=api_key)
 
                completion = await client.chat.completions.create(
                    messages=[
                        {"role": "system", "content": "You are a precise JSON extractor."},
                        {"role": "user", "content": prompt}
                    ],
                    model="llama-3.3-70b-versatile",
                    temperature=0.1, # Low temperature for factual accuracy
                    response_format={"type": "json_object"} # Force JSON mode
                )
 
                response_content = completion.choices[0].message.content
                return json.loads(response_content)
 
            except Exception as e:
                print(f" Groq Attempt {attempt+1} Failed for {company_name}: {e}")
                await asyncio.sleep(1)
 
        # If all retries fail, return empty placeholders
        return {
            "Official_Domain": "Not Found",
            "CEO_Full_Name": "Not Found",
            "CEO_First_Name": "",
            "CEO_Last_Name": "",
            "Company_URL": "",
            "LinkedIn_URL": ""
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
       
        # Merge Company Name into the result
        extracted_data["Company_Name"] = company_name
       
        # Add the EMPTY Email column for the user to fill
        extracted_data["Email_ID"] = ""
       
        return extracted_data
 
    except Exception as e:
        print(f" Error processing file {file_path}: {e}")
        return None
 
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
 
    # 4. Create DataFrame and Reorder Columns
    df = pd.DataFrame(valid_results)
   
    # Desired Column Order for the User CSV
    cols = [
        "Company_Name",
        "Official_Domain",
        "Company_URL",
        "CEO_Full_Name",
        "CEO_First_Name",
        "CEO_Last_Name",
        "LinkedIn_URL",
        "Email_ID"  # This will be empty, waiting for user input
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