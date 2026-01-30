

import asyncio
import os
import json
import time
from datetime import datetime
from typing import List, Dict, Any

# Third-party imports
from langchain_community.tools.tavily_search import TavilySearchResults
from dotenv import load_dotenv

# --- 1. SETUP & CONFIGURATION ---

# Load environment variables
load_dotenv()

# Define domains for high-quality news filtering
TARGET_DOMAINS = [
    "linkedin.com", "crunchbase.com", "clutch.co", "goodfirms.co",
    "g2.com", "yourstory.com", "inc42.com", "entrackr.com",
    "medium.com", "prlog.org", "businesswire.com", "finance.yahoo.com"
]

def get_api_keys(prefix: str) -> List[str]:
    """
    Retrieves a list of API keys from environment variables based on a prefix.
    """
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

# Load Keys
TAVILY_KEYS = get_api_keys("TAVILY_API_KEY")

if not TAVILY_KEYS:
    raise ValueError(" CRITICAL: No TAVILY_API_KEYs found in .env file!")

# --- 2. RETRY & ROTATION LOGIC ---

def run_tavily_with_retry(query: str, max_results: int = 5, domains: List[str] = None) -> List[Dict]:
    """
    Executes a Tavily search with automatic API key rotation and error handling.
    """
    tool_args = {
        "max_results": max_results,
        "search_depth": "advanced",
        "include_answer": True,
        "include_raw_content": True
    }
    
    if domains:
        tool_args["include_domains"] = domains

    for index, api_key in enumerate(TAVILY_KEYS):
        try:
            os.environ["TAVILY_API_KEY"] = api_key
            tool = TavilySearchResults(**tool_args)
            return tool.invoke(query)

        except Exception as e:
            print(f"       Tavily Key {index+1} Failed: {e}")
            if index < len(TAVILY_KEYS) - 1:
                print("       Switching to next Tavily Key...")
                time.sleep(2) 
            else:
                print("       All Tavily Keys exhausted.")
                return []
    return []

# --- 3. SYNCHRONOUS CORE LOGIC (Data Fetching) ---

def fetch_financial_data(company_name: str) -> List[Dict]:
    print(f"[*] Fetching financial data for: {company_name}...")
    query = f"Find detailed financial information for '{company_name}': Revenue, Funding, Employees, CEO."
    return run_tavily_with_retry(query, max_results=5)

def fetch_company_news(company_name: str) -> List[Dict]:
    print(f"[*] Fetching news for: {company_name}...")
    query = f"Recent news for '{company_name}': Partnerships, Launches, Funding."
    return run_tavily_with_retry(query, max_results=5, domains=TARGET_DOMAINS)

def generate_report(company_name: str) -> str:
    print(f"\n---  STARTING ANALYSIS: {company_name.upper()} ---")
    
    financial_data = fetch_financial_data(company_name)
    news_data = fetch_company_news(company_name)
    
    report_payload = {
        "meta": {
            "company_name": company_name,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "financial_intelligence": financial_data,
        "market_updates": news_data
    }
    
    output_dir = "Unstructured_data"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"{company_name.replace(' ', '_')}_Report.json")

    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(report_payload, f, indent=4, ensure_ascii=False)
        print(f" COMPLETED: {company_name}")
        return filename
    except Exception as e:
        print(f" FILE ERROR {company_name}: {e}")
        return ""

# --- 4. ASYNC ORCHESTRATION LAYER ---

# [FIX] Global Semaphore 'sem' removed from here.

async def process_company_task(company_name: str, sem: asyncio.Semaphore):
    """
    Wrapper function that accepts the semaphore as an argument to ensure 
    it belongs to the current active event loop.
    """
    async with sem: # Enforce concurrency limit properly
        try:
            # Run blocking I/O in a separate thread
            await asyncio.to_thread(generate_report, company_name)
        except Exception as e:
            print(f" Unhandled Error for {company_name}: {e}")

async def run_deep_research_for_companies(company_list: List[str]):
    """
    Main Async Entry Point: Creates the semaphore here to bind it to the correct loop.
    """
    # [FIX] Initialize Semaphore inside the async function
    sem = asyncio.Semaphore(3) 
    
    print(f" Starting Deep Research for {len(company_list)} companies...")
    print(f" Parallel Concurrency Limit: 3 threads")
    
    start_time = time.time()

    tasks = []
    for company in company_list:
        # [FIX] Pass the semaphore into the task function
        tasks.append(process_company_task(company, sem))

    # Run all tasks concurrently
    await asyncio.gather(*tasks)

    end_time = time.time()
    duration = end_time - start_time
    print(f"\n All Operations Finished in {duration:.2f} seconds!")

# --- 5. EXECUTION ---

if __name__ == "__main__":
    test_companies = ["Salesforce", "HubSpot", "Zoho", "Freshworks", "Zendesk"]
    asyncio.run(run_deep_research_for_companies(test_companies))