
import streamlit as st
import pandas as pd
import plotly.express as px
from serpapi import GoogleSearch
from datetime import datetime, timedelta
import logging
import requests
import time
import asyncio
# from instantly_mail_send import *
import os
from company_intel import enrich_companies_from_list
from deep_company_research import run_deep_research_for_companies
from company_cleaner import clean_all_unstructured_reports_async
from upload_to_sheets import upload_batch_data
from lead_scoring import run_ai_strategic_layer
import json
from pathlib import Path
from API_rotation import get_serpapi_key,get_serpapi_count
from email_generation import run_email_generation_layer
from Data_Enrichment import run_data_enrichment
from email_generation import connect_to_sheet, GOOGLE_SHEET_NAME
import gspread
SEEN_JOBS_FILE = "seen_jobs.json"

if "show_old_jobs" not in st.session_state:
    st.session_state.show_old_jobs = False

COMPANY_INTEL_FILE = os.path.join(
    "company_intel",
    "Final_Company_Data_by_simple_approach.json"
)

intel_dir = os.path.dirname(COMPANY_INTEL_FILE)
if not os.path.exists(intel_dir):
    os.makedirs(intel_dir, exist_ok=True)
    print(f"Created directory: {intel_dir}")


if not os.path.exists(COMPANY_INTEL_FILE):
    with open(COMPANY_INTEL_FILE, "w", encoding="utf-8") as f:
        json.dump({}, f) 
    print(f"Created empty intel file: {COMPANY_INTEL_FILE}")

import copy

def get_file_mtime(path: str) -> float:
    return os.path.getmtime(path) if os.path.exists(path) else 0.0


# def load_company_intel(path) -> dict:
#     with open(COMPANY_INTEL_FILE, "r", encoding="utf-8") as f:
#         data = json.load(f)



def load_company_intel(path) -> dict:
    # Check if the file actually exists before trying to open it
    if not os.path.exists(COMPANY_INTEL_FILE):
        logger.warning(f"Intelligence file not found at {COMPANY_INTEL_FILE}. Starting with empty intel.")
        return {} 

    try:
        with open(COMPANY_INTEL_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return copy.deepcopy(data)
    except Exception as e:
        logger.error(f"Error loading company intel: {e}")
        return {}

    # 🔒 CRITICAL: prevent mutation bugs
    return copy.deepcopy(data)


if 'show_leads' not in st.session_state:
    st.session_state.show_leads = False
# ================= LOGGER =================
def get_logger():
    logger = logging.getLogger("job_scrapper")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    try:
        handler = logging.FileHandler("app.log", mode='w', encoding="utf-8")
    except:
        handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

logger = get_logger()
logger.info("Application started")

# ================= CONFIGURATION =================
SERPAPI_KEYS = [
    os.getenv("SERPAPI_KEY_1"),
    os.getenv("SERPAPI_KEY_2")
]

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
 
ADZUNA_COUNTRY_MAP = {
    "India": "in", "United Kingdom": "gb", "United States": "us", "Canada": "ca",
    "Australia": "au", "Germany": "de", "France": "fr", "Brazil": "br",
    "South Africa": "za", "Singapore": "sg", "Netherlands": "nl", "Italy": "it",
    "Poland": "pl", "Austria": "at", "New Zealand": "nz", "Mexico": "mx"
}

# ================= GL & HL MAPPING =================
COUNTRY_GL_HL_MAP = {
    "United States": ("us", "en"), "USA": ("us", "en"), "US": ("us", "en"), "America": ("us", "en"),
    "New York": ("us", "en"), "California": ("us", "en"),
    "United Kingdom": ("gb", "en"), "UK": ("gb", "en"), "Britain": ("gb", "en"), "England": ("gb", "en"),
    "London": ("gb", "en"),
    "UAE": ("ae", "en"), "United Arab Emirates": ("ae", "en"), "Dubai": ("ae", "en"), "Abu Dhabi": ("ae", "en"),
    "France": ("fr", "fr"), "Paris": ("fr", "fr"),
    "Germany": ("de", "de"), "Berlin": ("de", "de"), "Deutschland": ("de", "de"),
    "India": ("in", "en"), "Mumbai": ("in", "en"), "Delhi": ("in", "en"),
    "Bangalore": ("in", "en"), "Bengaluru": ("in", "en"), "Hyderabad": ("in", "en"),
    "Canada": ("ca", "en"), "Toronto": ("ca", "en"), "Vancouver": ("ca", "en"), "Montreal": ("ca", "en"),
    "Australia": ("au", "en"), "Sydney": ("au", "en"), "Melbourne": ("au", "en"),
    "Singapore": ("sg", "en"),
    "Netherlands": ("nl", "nl"), "Amsterdam": ("nl", "nl"), "Holland": ("nl", "nl"),
    "Switzerland": ("ch", "de"), "Zurich": ("ch", "de"), "Geneva": ("ch", "de"),
    "Sweden": ("se", "sv"), "Stockholm": ("se", "sv"),
    "Ireland": ("ie", "en"), "Dublin": ("ie", "en"),
    "Israel": ("il", "en"), "Tel Aviv": ("il", "en"),
    "Saudi Arabia": ("sa", "ar"), "Riyadh": ("sa", "ar"), "KSA": ("sa", "ar"),
    "Qatar": ("qa", "ar"), "Doha": ("qa", "ar"),
    "Oman": ("om", "ar"), "Muscat": ("om", "ar"),
    "Bahrain": ("bh", "ar"),
}

st.set_page_config(page_title="Intelligence Lead Dashboard", layout="wide")

# ================= CUSTOM CSS =================
st.markdown("""
<style>
/* App background */
.stApp { background-color: #f8f9fa; }

/* Sidebar background */
section[data-testid="stSidebar"] { 
    background-color: #1e1e1e !important; 
}

/* Sidebar labels & captions stay white */
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3,
section[data-testid="stSidebar"] span {
    color: white !important;
}

/* 🔥 SELECTED VALUE inside dropdowns (Timeline, Job Nature) */
section[data-testid="stSidebar"] div[data-baseweb="select"] > div {
    color: black !important;
    background-color: white !important;
}

/* Dropdown menu items */
section[data-testid="stSidebar"] ul li {
    color: black !important;
}

/* Slider value text */
section[data-testid="stSidebar"] .stSlider span {
    color: white !important;
}

/* Buttons */
.stButton>button {
    background-color: #10a37f;
    color: white;
    border-radius: 8px;
    font-weight: 600;
}
</style>
""", unsafe_allow_html=True)




# ================= COUNTRY HELPERS =================

 
#---------------------------------
#Adzuna setup
 
def parse_adzuna_location(user_input):
    parts = [p.strip() for p in user_input.split(",")]
    detected_country = detect_search_country(user_input)
    country_code = ADZUNA_COUNTRY_MAP.get(detected_country)
   
    if len(parts) > 1:
        target_city = parts[1] if parts[0].lower() == detected_country.lower() else parts[0]
    else:
        target_city = user_input
       
    return country_code, target_city, detected_country
 


def load_seen_jobs():
    if os.path.exists(SEEN_JOBS_FILE):
        with open(SEEN_JOBS_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_seen_jobs(seen):
    with open(SEEN_JOBS_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(seen), f, indent=2)



def get_high_score_companies(company_df, threshold=10):
    return (
        company_df[company_df["Lead Score"] >= threshold]["Company"]
        .dropna()
        .unique()
        .tolist()
    )

def detect_search_country(user_input):
    if not user_input:
        return "United States"
    text = user_input.lower()
    for c in COUNTRY_GL_HL_MAP:
        if c.lower() in text:
            return c
    return "United States"

def extract_country(location_str):
    if not location_str:
        return "Unknown"
    loc = location_str.lower()
    if "remote" in loc or "anywhere" in loc:
        return "Remote"
    parts = [p.strip() for p in location_str.split(",")]
    last = parts[-1].upper()
    if last in ["USA", "UK", "UAE", "KSA"]:
        return {"USA": "United States", "UK": "United Kingdom", "UAE": "UAE", "KSA": "Saudi Arabia"}.get(last, last.title())
    return last.title() if len(parts) > 1 else "Unknown"

# # ================= SERPAPI (Google Jobs) - WITH FILTERS =================


def get_leads_serpapi(q, loc, date_f, type_f, limit):
    detected_country = detect_search_country(loc)
    gl, hl = COUNTRY_GL_HL_MAP.get(detected_country, ("us", "en"))

    all_jobs, seen = [], set()
    token = None

    # Handle Chips (Filters)
    chips = []
    if date_f != "All":
        chips.append(f"date_posted:{date_f}")
    if type_f != "All":
        chips.append(f"employment_type:{type_f}")
    chips_q = ",".join(chips) if chips else None

    # Loop until we hit the limit
    while len(all_jobs) < limit:
        
        # 1. Get a fresh key from the manager
        try:
            current_api_key = get_serpapi_key()
        except ValueError as e:
            logger.error(f"SerpAPI Key Error: {e}")
            break 

        params = {
            "engine": "google_jobs",
            "q": q,
            "location": loc,
            "gl": gl,
            "hl": hl,
            "chips": chips_q,
            "api_key": current_api_key, # <--- Dynamic Key
            "no_cache": False
        }
        
        if token:
            params["next_page_token"] = token

        try:
            search = GoogleSearch(params)
            res = search.get_dict()
            
            # Check for API-side errors (like invalid key or quota limit)
            if "error" in res:
                logger.warning(f"SerpAPI Key Failed: {res['error']}")
                time.sleep(1) # Wait briefly before rotating to next key
                continue

            jobs = res.get("jobs_results", [])

            # Logic: If no jobs found, try next key (rotation happens automatically on next loop)
            if not jobs:
                if not token:
                    # If no jobs and no next page, we are likely done.
                    # But optionally, you can 'continue' here to try one more key just in case.
                    break 
                else:
                    continue

            for j in jobs:
                key = f"{j.get('title')}-{j.get('company_name')}-{j.get('location')}"
                if key in seen:
                    continue
                seen.add(key)

                all_jobs.append({
                    "Job Title": j.get("title"),
                    "Company": j.get("company_name"),
                    "Location": j.get("location"),
                    "Country": extract_country(j.get("location")),
                    "Type": j.get("job_type", "Not Specified"),
                    "Market Source": "Google Jobs (SerpAPI)",
                    "Posted": j.get("detected_extensions", {}).get("posted_at", "Recent"),
                    "Apply Link": j.get("apply_options", [{}])[0].get("link"),
                    "Job Description": j.get("description", "Not available (basic view)"),
                    "Company URL": j.get("via", "Not available")
                })

                if len(all_jobs) >= limit:
                    break

            # Handle Pagination
            token = res.get("serpapi_pagination", {}).get("next_page_token")
            if not token and len(jobs) < 10:
                break 

        except Exception as e:
            logger.error(f"SerpAPI error: {e}")
            time.sleep(1) # Wait before retrying (which will grab a new key)

    return all_jobs
# ================= LINKEDIN (RapidAPI) - WITH JOB TYPE FILTER =================
import requests

def get_leads_linkedin(q, loc, date_f, type_f, limit):
    url = "https://jobs-api14.p.rapidapi.com/v2/linkedin/search"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "jobs-api14.p.rapidapi.com"
    }

    # Map date_f to LinkedIn datePosted values (approximate; API supports day/week/month)
    date_map = {
        "All": None,
        "today": "day",
        "3days": "week",  # Closest match
        "week": "week",
        "month": "month"
    }
    date_posted = date_map.get(date_f)

    # Map type_f to LinkedIn employmentTypes
    type_map = {
        "FULLTIME": "fulltime",
        "CONTRACTOR": "contractor",
        "INTERN": "intern",
        "All": "fulltime;contractor;parttime;intern;temporary"
    }
    employment_types = type_map.get(type_f, type_map["All"])

    # Base params
    params = {
        "query": q,
        "location": loc or "Worldwide",
        "workplaceTypes": "remote;hybrid;onSite",
        "employmentTypes": employment_types,
        "experienceLevels": "intern;entry;associate;midSenior;director",  # Always include broad experience levels
        "limit": min(limit, 50)  # API limit per page; adjust as needed
    }

    if date_posted:
        params["datePosted"] = date_posted

    all_jobs, seen = [], set()
    next_token = None

    while len(all_jobs) < limit:
        if next_token:
            params["token"] = next_token

        try:
            res = requests.get(url, headers=headers, params=params)
            res.raise_for_status()
            data = res.json()
            jobs = data.get("data", [])

            if not jobs:
                break

            for j in jobs:
                key = f"{j.get('title')}-{j.get('companyName')}-{j.get('location')}"
                if key in seen:
                    continue
                seen.add(key)

                all_jobs.append({
                    "Job Title": j.get("title"),
                    "Company": j.get("companyName", "Unknown"),
                    "Location": j.get("location"),
                    "Country": extract_country(j.get("location")),
                    "Type": j.get("employmentType", type_f if type_f != "All" else "Not Specified"),
                    "Market Source": "LinkedIn (RapidAPI)",
                    "Posted": j.get("postedTimeAgo", j.get("datePosted", "Unknown")),
                    "Apply Link": j.get("applyUrl", f"https://www.linkedin.com/jobs/view/{j.get('id')}"),  # Fallback to LinkedIn job URL
                    "Job Description": j.get("description", "Not available (use /job-details endpoint for full desc)"),
                    "Company URL": j.get("companyUrl", "Not available")  # Add this line
                })

                if len(all_jobs) >= limit:
                    break

            next_token = data.get("meta", {}).get("nextToken")
            if not next_token:
                break

            time.sleep(0.5)  # Rate limiting

        except Exception as e:
            logger.error(f"LinkedIn API error: {e}")
            break

    return all_jobs[:limit]

def build_why_this_lead(row):
    return (
        f"Hiring {row['Open_Roles']} role(s) across {row['Countries']}, "
        f"indicating {row['Detected Need'].lower()}."
    )


# ================= JSEARCH - WITH JOB TYPE & DATE FILTER (via query) =================
def get_leads_jsearch(q, loc, date_f, type_f, limit):
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }
    search_url = "https://jsearch.p.rapidapi.com/search"
    details_url = "https://jsearch.p.rapidapi.com/job-details"

    # Enhance query with filters
    query_parts = [q, f"in {loc}"]

    if type_f != "All":
        type_map = {
            "FULLTIME": "full time",
            "CONTRACTOR": "contract",
            "INTERN": "internship"
        }
        query_parts.append(type_map.get(type_f, ""))

    if date_f != "All":
        date_map = {
            "today": "today",
            "3days": "past 3 days",
            "week": "past week",
            "month": "past month"
        }
        query_parts.append(date_map.get(date_f, ""))

    query = " ".join([p for p in query_parts if p])

    search_params = {
        "query": query,
        "page": "1",
        "num_pages": str((limit // 10) + 2)
    }

    try:
        response = requests.get(search_url, headers=headers, params=search_params)
        data = response.json()
        if data.get("status") != "OK":
            return []
        jobs = data.get("data", [])[:limit * 2]  # Get extra to filter
    except Exception as e:
        logger.error(f"JSearch search error: {e}")
        return []

    results = []
    seen = set()

    for job in jobs:
        job_id = job.get("job_id")
        key = f"{job.get('job_title')}-{job.get('employer_name')}-{job.get('job_location')}"
        if key in seen:
            continue
        seen.add(key)

        city = job.get("job_city") or ""
        state = job.get("job_state") or ""
        country = job.get("job_country") or ""
        location_parts = [p for p in [city, state, country] if p]
        location = ", ".join(location_parts) if location_parts else "Remote/Unknown"

        description = "Not fetched"
        try:
            details_resp = requests.get(details_url, headers=headers, params={"job_id": job_id})
            details_data = details_resp.json()
            if details_data.get("status") == "OK" and details_data.get("data"):
                detail = details_data["data"][0]
                description = detail.get("job_description", "Not available")
                time.sleep(0.2)
        except:
            description = "Error fetching details"

        results.append({
            "Job Title": job.get("job_title"),
            "Company": job.get("employer_name"),
            "Location": job.get("job_location", location),
            "Country": extract_country(job.get("job_location", "")),
            "Type": job.get("job_employment_type", "Not Specified"),
            "Market Source": "JSearch (Enhanced Google Jobs)",
            "Posted": job.get("job_posted_at", "Recent"),
            "Apply Link": job.get("job_apply_link"),
            "Job Description": description,
            "Company URL": job.get("employer_website", "Not available")
        })

        if len(results) >= limit:
            break

    return results

 
#---------------------------------------------------
# Adzuna get leads
 
def get_leads_adzuna(q, loc, date_f, type_f, limit):
    country_code, target_city, country_name = parse_adzuna_location(loc)
    all_jobs, seen = [], set()
    page, date_map = 1, {"All": None, "today": 1, "3days": 3, "week": 7, "month": 30}
    max_days = date_map.get(date_f)
 
    # Multi-Stage Search: Exact City first, then broader Country
    search_locations = [target_city, ""]
   
    for search_loc in search_locations:
        if len(all_jobs) >= limit: break
        page = 1
        while len(all_jobs) < limit and page <= 3:
            url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/{page}"
            params = {
                "app_id": ADZUNA_APP_ID,
                "app_key": ADZUNA_APP_KEY,
                "results_per_page": 50,
                "what": q,
                "where": search_loc,
                "max_days_old": max_days
            }
            try:
                res = requests.get(url, params=params, timeout=10)
                jobs = res.json().get("results", [])
                if not jobs: break
               
                for j in jobs:
                    title = j.get("title", "").lower()
                    if q.lower() not in title: continue # Quality filter
                       
                    job_loc = j.get("location", {}).get("display_name", search_loc or country_name)
                    key = f"{j.get('title')}-{j.get('company', {}).get('display_name')}-{job_loc}"
                   
                    if key in seen: continue
                    seen.add(key)
                   
                    all_jobs.append({
                        "Job Title": j.get("title"),
                        "Company": j.get("company", {}).get("display_name", "Unknown"),
                        "Location": job_loc,
                        "Country": country_name,
                        "Type": type_f if type_f != "All" else "Not Specified",
                        "Market Source": "Adzuna",
                        "Posted": j.get("created")[:10],
                        "Apply Link": j.get("redirect_url"),
                        "Job Description": j.get("description", "N/A"),
                        "Company URL": "N/A"
                    })
                    if len(all_jobs) >= limit: break
                page += 1
            except Exception as e:
                logger.error(f"Adzuna error: {e}")
                break
    return all_jobs
import re

def normalize_revenue(rev):
    """
    Normalize revenue strings to USD MILLIONS (float)

    Handles:
    - $5M, $5 million, 5m
    - $4.9B – $5.1B (takes midpoint)
    - ₹1000 crores, 1000 cr, 1000 crores+
    - € / £ (treated as USD approx)
    - noisy text, ranges, plus signs
    """

    if not rev or not isinstance(rev, str):
        return None

    r = rev.lower()
    r = r.replace(",", "").replace("+", "").replace("approx", "").replace("~", "")
    r = r.strip()

    # -----------------------------
    # Extract all numeric values
    # -----------------------------
    numbers = re.findall(r"\d+(?:\.\d+)?", r)
    if not numbers:
        return None

    nums = [float(n) for n in numbers]

    # If range exists → take midpoint
    value = sum(nums) / len(nums)

    # -----------------------------
    # Currency & unit detection
    # -----------------------------

    # INR Crores / Lakhs
    if "crore" in r or "cr" in r:
        return value * 0.12   # 1 crore ≈ 0.12M USD

    if "lakh" in r:
        return value * 0.0012  # 1 lakh ≈ 0.0012M USD

    # Billion
    if "billion" in r or re.search(r"\bb\b", r):
        return value * 1000

    # Million
    if "million" in r or re.search(r"\bm\b", r):
        return value

    # Explicit currency but no unit → assume millions
    if any(c in r for c in ["$", "€", "£"]):
        return value

    # Fallback heuristic
    # If number is very large, assume it's already in millions
    if value > 1000:
        return value

    return None




def normalize_employee_count(val):
    """
    Converts employee ranges or strings to a numeric midpoint.
    """
    if val is None:
        return None

    if isinstance(val, int):
        return val

    if not isinstance(val, str):
        return None

    v = val.lower().replace(",", "").strip()

    # "5000+"
    if v.endswith("+"):
        return int(v[:-1])

    # "201-500"
    if "-" in v:
        try:
            low, high = v.split("-")
            return (int(low) + int(high)) // 2
        except:
            return None

    # "1000"
    try:
        return int(v)
    except:
        return None
    




# def parse_posted_to_days(posted_str):
#     """
#     Converts 'Recent', '19 hours ago', '4 days ago', etc. → number of days ago
#     Uses UTC time (safe for Render / cloud deployments)
#     """
#     if not posted_str or not isinstance(posted_str, str):
#         return None

#     s = posted_str.lower().strip()

#     # Treat "recent" as today
#     if "recent" in s:
#         return 0

#     # Match "X hours ago"
#     hours_match = re.search(r"(\d+)\s*hour", s)
#     if hours_match:
#         hours = int(hours_match.group(1))
#         return max(hours / 24, 0)

#     # Match "X days ago"
#     days_match = re.search(r"(\d+)\s*day", s)
#     if days_match:
#         return int(days_match.group(1))

#     # Match "X weeks ago" (just in case)
#     weeks_match = re.search(r"(\d+)\s*week", s)
#     if weeks_match:
#         return int(weeks_match.group(1)) * 7

#     return None

def parse_posted_to_days(posted):
    """
    Handles:
    - '2 days ago', '19 hours ago', 'Recent'
    - 'YYYY-MM-DD' (Adzuna Fix)
    - pandas.Timestamp / datetime
    """
    if posted is None:
        return None

    # ---- Datetime / Timestamp (Adzuna via Pandas) ----
    if isinstance(posted, (datetime, pd.Timestamp)):
        days_ago = (datetime.utcnow() - posted.to_pydatetime()).days
        return max(days_ago, 0)

    # ---- String formats ----
    if not isinstance(posted, str):
        return None

    s = posted.lower().strip()

    # 1. Old Logic: Recent
    if "recent" in s:
        return 0

    # 2. Old Logic: Hours
    hours_match = re.search(r"(\d+)\s*hour", s)
    if hours_match:
        return int(hours_match.group(1)) / 24

    # 3. Old Logic: Days
    days_match = re.search(r"(\d+)\s*day", s)
    if days_match:
        return int(days_match.group(1))

    # 4. Old Logic: Weeks
    weeks_match = re.search(r"(\d+)\s*week", s)
    if weeks_match:
        return int(weeks_match.group(1)) * 7

    # ---- NEW LOGIC: ISO date string (For Adzuna) ----
    # This runs only if the regex matches above failed.
    # It attempts to parse 'YYYY-MM-DD' format.
    try:
        posted_date = datetime.strptime(s[:10], "%Y-%m-%d")
        days_ago = (datetime.utcnow() - posted_date).days
        return max(days_ago, 0)
    except:
        return None


def job_freshness_score(posted_str):
    days_ago = parse_posted_to_days(posted_str)

    if days_ago is None:
        return 0

    if days_ago <= 10:
        return 5
    elif days_ago <= 20:
        return 2.5
    else:
        return 1


def revenue_match_score(val, user_choice):
    if val is None or user_choice == "Any":
        return 0

    ranges = {
        "$1M – $10M": (1, 10),
        "$10M – $25M": (10, 25),
        "$25M – $50M": (25, 50),
        "$50M – $100M": (50, 100),
        "$100M – $250M": (100, 250),
        "$250M – $500M": (250, 500),
        "$500M – $1B": (500, 1000),
    }

    low, high = ranges[user_choice]

    if low <= val <= high:
        return 5
    if low * 0.85 <= val <= high * 1.15:
        return 2.5
    return 0
def employee_match_score(val, user_choice):
    val = normalize_employee_count(val)

    if val is None or user_choice == "Any":
        return 0

    ranges = {
        "1–10": (1, 10),
        "11–20": (11, 20),
        "21–50": (21, 50),
        "51–100": (51, 100),
        "101–200": (101, 200),
        "201–500": (201, 500),
        "501–1000": (501, 1000),
        "1001–5000": (1001, 5000),
        "5001–10000": (5001, 10000),
    }

    low, high = ranges[user_choice]

    if low <= val <= high:
        return 5
    if low * 0.85 <= val <= high * 1.15:
        return 2.5
    return 0

def final_lead_score(row, intel, revenue_q, size_q):
    score = 0

    # 1️ Vacancy score
    score += row["Open_Roles"] * 5

    # 2️ Intent score (keep existing)
    intent_bonus = {
        "CRM Migration": 25,
        "System Integration": 20,
        "Salesforce Optimization": 15,
        "Ongoing Salesforce Support": 10,
        "Salesforce Expansion": 10
    }
    score += intent_bonus.get(row["Detected Need"], 0)

    # 3️ Company intelligence
    company = row["Company"]
    if company in intel:
        rev = normalize_revenue(intel[company].get("Annual Revenue"))
        emp = intel[company].get("Total Employee Count")

        score += revenue_match_score(rev, revenue_q)
        score += employee_match_score(emp, size_q)

    return min(score, 100)


# ================= LEAD INTELLIGENCE HELPERS =================
def detect_need(text):
    t = text.lower()

    if any(k in t for k in ["migration", "migrate", "transition", "move from"]):
        return "CRM Migration"

    if any(k in t for k in ["optimize", "optimization", "performance", "improve"]):
        return "Salesforce Optimization"

    if any(k in t for k in ["integration", "api", "erp", "sap", "oracle"]):
        return "System Integration"

    if any(k in t for k in ["admin", "support", "managed services"]):
        return "Ongoing Salesforce Support"

    return "Salesforce Expansion"



def final_lead_score_salesforce(row, intel, revenue_q, size_q):
    score = 0
    breakdown = []
    
    company = row["Company"]

    # ===============================
    # SAFETY: Company must exist in intel
    # ===============================
    if company not in intel:
        return 0, " No company intelligence found"

    # ===============================
    # NORMALIZE COMPANY DATA
    # ===============================
    rev = normalize_revenue(intel[company].get("Annual Revenue"))
    emp = normalize_employee_count(
        intel[company].get("Total Employee Count")
    )

    # ===============================
    # HARD FILTER: REVENUE
    # ===============================
    if revenue_q != "Any":
        rev_score = revenue_match_score(rev, revenue_q)
        if rev_score == 0:
            return 0, f" Revenue outside selected range ({revenue_q})"
        breakdown.append(f"+{rev_score} (Revenue Match)")
        score += rev_score

    # ===============================
    # HARD FILTER: EMPLOYEE SIZE
    # ===============================
    if size_q != "Any":
        emp_match = employee_match_score(emp, size_q)
        if emp_match == 0:
            return 0, f" Employee size outside selected range ({size_q})"
        breakdown.append(f"+{emp_match} (Employee Size Match)")
        score += emp_match

    # ===============================
    # 1️⃣ JOB VOLUME SCORE (FIXED)
    # ===============================
    open_roles = int(row.get("Open_Roles", 0))
    volume_score = min(open_roles * 5, 20)  # 2 jobs = 10 points
    score += volume_score
    breakdown.append(f"+{volume_score} (Job Volume)")

    # ===============================
    # 2️⃣ SALESFORCE CLOUD RELEVANCE
    # ===============================
    cloud_score = salesforce_cloud_score(row.get("Job_Roles", []))
    if cloud_score > 0:
        score += cloud_score
        breakdown.append(f"+{cloud_score} (Salesforce Clouds)")

    # ===============================
    # 3️⃣ JOB FRESHNESS
    # ===============================
    freshness = row.get("Freshness Score", 0)
    if freshness > 0:
        score += freshness
        breakdown.append(f"+{freshness} (Freshness)")

    # ===============================
    # FINAL SCORE
    # ===============================
    return min(score, 100), " | ".join(breakdown)

def calculate_lead_score(row):
    score = 0

    score += min(row["Open_Roles"] * 20, 60)

    intent_bonus = {
        "CRM Migration": 25,
        "System Integration": 20,
        "Salesforce Optimization": 15,
        "Ongoing Salesforce Support": 10,
        "Salesforce Expansion": 10
    }

    score += intent_bonus.get(row["Detected Need"], 0)
    return min(score, 100)


def update_structured_json_with_scores(company_df, search_job_role, search_location, structured_dir="structured_data"):
    from pathlib import Path
    import json

    structured_dir = Path(structured_dir)

    if not structured_dir.exists():
        os.makedirs(structured_dir, exist_ok=True)
        return

    # --- UPDATED: Build lookup to include Open Roles ---
    score_map = {
        row["Company"].strip().lower(): {
            "lead_score": float(row["Lead Score"]),
            "rank_breakout": row["Rank (Breakout)"],
            "specific_jobs": row["Job_Roles"],
            # NEW: Add Open Roles Count here
            "open_roles": int(row["Open_Roles"]) 
        }
        for _, row in company_df.iterrows()
        if pd.notna(row["Company"])
    }

    updated = 0

    for file in structured_dir.glob("*_Structured.json"):
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)

            company_name = (
                data.get("meta_company_name")
                or data.get("company_profile_company_name")
                or data.get("meta", {}).get("company_name")
                or ""
            ).strip().lower()

            if not company_name or company_name not in score_map:
                continue

            # 1. Inject Search Context
            data["search_context"] = {
                "target_job_role": search_job_role,
                "target_location": search_location
            }

            # 2. Inject Scoring
            data["lead_scoring"] = {
                "lead_score": score_map[company_name]["lead_score"],
                "rank_breakout": score_map[company_name]["rank_breakout"]
            }

            # 3. Inject Specific Job Titles
            job_list = score_map[company_name]["specific_jobs"]
            if isinstance(job_list, list):
                job_string = ", ".join(job_list)
            else:
                job_string = str(job_list)
            
            data["found_job_titles"] = job_string

            # --- 4. NEW: Inject Open Roles Count ---
           
            data["open_roles_count"] = score_map[company_name]["open_roles"]
            # ---------------------------------------

            with open(file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

            updated += 1

        except Exception as e:
            print(f" Failed updating {file.name}: {e}")

    print(f" Context, Jobs, Open Roles & Scoring injected into {updated} structured JSON files")
def job_volume_score(open_roles):
    if open_roles <= 2:
        return 5
    elif 3 <= open_roles <= 5:
        return 15
    else:
        return 20

def salesforce_cloud_score(job_roles):
    score = 0
    keywords = {
        "sales cloud": 8,
        "service cloud": 8,
        "marketing cloud": 7,
        "data cloud": 7,
        "industry cloud": 7
    }

    for role in job_roles:
        role_l = role.lower()
        for key, val in keywords.items():
            if key in role_l:
                score += val

    return score

def employee_size_score(emp):
    emp = normalize_employee_count(emp)

    if emp is None:
        return 0
    if emp <= 20:
        return 8
    elif emp <= 50:
        return 7
    elif emp <= 100:
        return 3
    else:
        return 2



def load_uploaded_companies(uploaded_file):
    if uploaded_file is None:
        return []

    try:
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        df.columns = [c.strip().lower() for c in df.columns]

        if "company" not in df.columns:
            st.error(" Uploaded file must contain a 'Company' column")
            return []

        companies = (
            df["company"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        return companies

    except Exception as e:
        st.error(f"❌ Failed to read uploaded file: {e}")
        return []

#----------New Logic code ---------------------

# ================= NEW: SYNC ENRICHED DATA TO SHEETS =================
def update_sheet_with_enriched_data(uploaded_df):
    """
    Updates Google Sheet with verified CEO Name and Email ID from user's CSV.
    """
    gc = connect_to_sheet()
    if not gc:
        st.error(" Failed to connect to Google Sheets.")
        return False

    try:
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.sheet1
        
        # Get all existing records to map rows
        sheet_data = worksheet.get_all_records()
        if not sheet_data:
            st.warning(" Google Sheet is empty.")
            return False
            
        # Create a mapping of Company Name -> Row Number (starting at 2 due to header)
        # Assuming column name in Sheet is 'meta_company_name' or 'Company'
        company_row_map = {}
        for idx, row in enumerate(sheet_data):
            c_name = str(row.get("meta_company_name", "") or row.get("Company", "")).strip().lower()
            if c_name:
                company_row_map[c_name] = idx + 2  # +2 because gspread is 1-indexed and row 1 is header

        # Find or Create Columns for 'CEO Name' and 'Email ID'
        headers = worksheet.row_values(1)
        
        if "CEO Name" not in headers:
            worksheet.update_cell(1, len(headers) + 1, "CEO Name")
            headers.append("CEO Name")
            
        if "Email ID" not in headers:
            worksheet.update_cell(1, len(headers) + 1, "Email ID")
            headers.append("Email ID")

        ceo_col_idx = headers.index("CEO Name") + 1
        email_col_idx = headers.index("Email ID") + 1

        # Iterate through uploaded CSV and update Sheet
        updates = 0
        for _, row in uploaded_df.iterrows():
            company = str(row.get("Company_Name", "")).strip().lower()
            ceo_name = str(row.get("CEO_Full_Name", "")).strip()
            email = str(row.get("Email_ID", "")).strip()

            # Skip if no email provided (optional, depends on your logic)
            if not email: 
                continue

            if company in company_row_map:
                row_num = company_row_map[company]
                # Update cells
                worksheet.update_cell(row_num, ceo_col_idx, ceo_name)
                worksheet.update_cell(row_num, email_col_idx, email)
                updates += 1
                time.sleep(1) # Rate limit safety
        
        return updates

    except Exception as e:
        st.error(f" Error updating sheet: {e}")
        return 0


## ================= SIDEBAR (PROFESSIONAL STATE MANAGEMENT) =================
with st.sidebar:
    st.title("⚙️ Search Logic")
   
    # 1. Job Source Provider
    provider = st.radio(
        "Job Source",
        ["SerpAPI (Google Jobs)", "LinkedIn (RapidAPI)", "JSearch (Enhanced Google Jobs)", "Adzuna (High Limit)"],
        index=0
    )
 
    # 2. Logic to handle "One-time" value jump for Adzuna
    if "last_provider" not in st.session_state:
        st.session_state.last_provider = provider
 
    # Automatic trigger when switching to Adzuna
    if provider.startswith("Adzuna") and st.session_state.last_provider != provider:
        st.session_state.target_count = 200 # Set exactly 200 for Adzuna
        st.session_state.last_provider = provider
    # Reset to standard 50 when switching back
    elif not provider.startswith("Adzuna") and st.session_state.last_provider != provider:
        st.session_state.target_count = 50
        st.session_state.last_provider = provider
 
    # 3. Dynamic Selection Boxes
    date_val = st.selectbox("📅 Timeline (Freshness)", ["All", "today", "3days", "week", "month"])
    type_val = st.selectbox("💼 Job Nature", ["All", "FULLTIME", "CONTRACTOR", "INTERN"])
 
    # 4. Smart Slider Logic
    # We set the max to 200 for Adzuna as requested
    max_limit = 200 if provider.startswith("Adzuna") else 100
   
    target = st.slider(
        "🎯 Target Leads Count",
        min_value=1,
        max_value=max_limit,
        value=st.session_state.get("target_count", 50),
        key="target_slider"
    )
   
    # Update session state with manual slider changes
    st.session_state.target_count = target
 
    st.divider()
    if provider.startswith("Adzuna"):
        st.caption("🚀 Adzuna Mode: Optimized for 200 leads")
    else:
        st.caption("✅ Standard Filters Active")
 
# ================= MAIN UI =================
st.title("🚀 Market Intelligence Dashboard")
st.markdown("Automated Talent Acquisition & Market Analysis Tool")

st.markdown("## 📥 Alternative Input: Upload Company File")

uploaded_file = st.file_uploader(
    "Upload CSV or Excel file (must contain a 'Company' column)",
    type=["csv", "xlsx"],
    help="Uploaded companies will be used to generate deep company intelligence and uploaded to Google Sheets."
)

if uploaded_file:
    st.info(
        "ℹ️ The uploaded file will be used to generate company intelligence "
        "and automatically uploaded to Google Sheets."
    )

    if st.button("🚀 Generate Company Intelligence from Uploaded File"):
        companies = load_uploaded_companies(uploaded_file)

        if companies:
            progress_text = st.empty()
            progress_bar = st.progress(0)

            # STEP 1: Deep Research
            progress_text.text("🔍 Running deep company research...")
            asyncio.run(run_deep_research_for_companies(companies))
            progress_bar.progress(50)

            # STEP 2: Cleaning
            progress_text.text("🧹 Cleaning & structuring company intelligence...")
            asyncio.run(clean_all_unstructured_reports_async(
                unstructured_dir="Unstructured_data",
                structured_dir="structured_data"
            ))
            progress_bar.progress(80)

            # STEP 3: Upload
            progress_text.text(" Uploading structured data to Google Sheets...")
           
            struct_dir = Path("structured_data")
            files_to_sync = list(struct_dir.glob("*_Structured.json"))
            
            if files_to_sync:
                # Naye Batch Function ko call karein
                upload_batch_data(files_to_sync)
                progress_bar.progress(100)
                st.success(f" Synced {len(files_to_sync)} companies to Google Sheets!")
            else:
                st.warning(" No structured files found to upload.")
            st.success(" Uploaded file processed and synced to Google Sheets!")



# 1. Initialize Session State for Data Storage
if 'df' not in st.session_state:
    st.session_state.df = None
if 'company_df' not in st.session_state:
    st.session_state.company_df = None
if 'show_leads' not in st.session_state:
    st.session_state.show_leads = False

c1, c2, c3, c4 = st.columns(4)

with c1:
    job_q = st.text_input(
        " Target Job Role",
        placeholder="e.g. Salesforce Developer"
    )

# with c2:
#     loc_q = st.text_input(
#         " Market Location",
#         placeholder="e.g. India, Germany, Dubai"
#     )

with c2:
    loc_q = st.text_input(
        " Market Location",
        placeholder="e.g. India, Germany, Dubai"
    )
   
if provider.startswith("Adzuna"):
       
    supported_countries = ", ".join(ADZUNA_COUNTRY_MAP.keys())
    st.caption(f" Works best for: {supported_countries}")
 

with c3:
    st.session_state.show_old_jobs = st.toggle(
        " Show old jobs",
        value=False,
        help="Include jobs you've already seen in previous searches"
    )


# ================= RUN SEARCH =================
if st.button("Generate Final Report"):
    if not job_q.strip() or not loc_q.strip():
        st.error("Please fill in both Job Role and Location.")
    else:
        with st.spinner(f"Fetching up to {target} jobs via {provider}..."):
            if provider.startswith("SerpAPI"):
                results = get_leads_serpapi(job_q, loc_q, date_val, type_val, target)
            elif provider.startswith("LinkedIn"):
                results = get_leads_linkedin(job_q, loc_q, date_val, type_val, target)
            elif provider.startswith("Adzuna"): 
                results = get_leads_adzuna(job_q, loc_q, date_val, type_val, target)
            else:
                results = get_leads_jsearch(job_q, loc_q, date_val, type_val, target)

        if not results:
            st.warning("No jobs found. Try broadening filters or switching source.")
            st.session_state.df = None
        else:
            # Create DataFrames
            df = pd.DataFrame(results)
            df["Apply Link"] = (
                df["Apply Link"]
                .astype(str)
                .str.strip()
                .str.split("?")
                .str[0]
            )
            seen_jobs = load_seen_jobs()

            if not st.session_state.show_old_jobs:
                df = df[~df["Apply Link"].isin(seen_jobs)]
            #  Mark ONLY the jobs that are actually shown as seen
            newly_shown_links = (
                df["Apply Link"]
                .dropna()
                .unique()
                .tolist()
            )

            seen_jobs.update(newly_shown_links)
            save_seen_jobs(seen_jobs)


            df["Company"] = df["Company"].astype(str).replace(
                {"None": "Unknown", "nan": "Unknown", "[]": "Unknown", "{}": "Unknown"}
            )
            
            # Aggregation
            company_df = (
                df.groupby("Company")
                .agg(
                    Job_Roles=("Job Title", lambda x: list(set(x))),
                    Open_Roles=("Job Title", "count"),
                    Locations=("Location", lambda x: ", ".join(set(x))),
                    Countries=("Country", lambda x: ", ".join(set(x))),
                    Job_Types=("Type", lambda x: ", ".join(set(x))),
                    Descriptions=("Job Description", lambda x: " ".join(x.astype(str))),
                )
                .reset_index()
            )
            company_df["Open_Roles"] = company_df["Job_Roles"].apply(len)
            company_df["Detected Need"] = company_df["Descriptions"].apply(detect_need)
            company_df["Why This Lead"] = (
                "Hiring "
                + company_df["Open_Roles"].astype(str)
                + " role(s) across "
                + company_df["Countries"].astype(str)
                + ", indicating "
                + company_df["Detected Need"].str.lower()
                + "."
            )
            # Freshness score based on most recent job per company
            company_df["Freshness Score"] = (
                df.groupby("Company")["Posted"]
                .apply(lambda x: max(job_freshness_score(p) for p in x if p))
                .values
            )

            company_df["Lead Score"] = company_df.apply(calculate_lead_score, axis=1)
            company_df = company_df.sort_values("Lead Score", ascending=False)
            

            #  STORE IN SESSION STATE
            st.session_state.df = df
            st.session_state.company_df = company_df
            st.session_state.show_leads = False # Reset filter view on new search

# ================= DISPLAY LOGIC =================
# We check if st.session_state.df has data. If it does, we show it.
if st.session_state.df is not None:
    df = st.session_state.df
    company_df = st.session_state.company_df

    # --- METRICS ---
    m1, m2, m3 = st.columns(3)
    m1.metric("Leads Identified", len(df))
    m2.metric("Unique Locations", df["Location"].nunique())
    m3.metric("Active Source", provider.split("(")[0].strip())

    # --- VISUALS ---
    col1, col2 = st.columns(2)
    with col1:
        top_companies = df["Company"].value_counts().head(10).reset_index()
        top_companies.columns = ["Company", "Open Roles"]
        st.plotly_chart(px.bar(top_companies, x="Company", y="Open Roles", title="🏢 Top Hiring Companies"), use_container_width=True)
    
    with col2:
        top_locations = df["Location"].value_counts().head(10).reset_index()
        top_locations.columns = ["Location", "Job Count"]
        st.plotly_chart(px.bar(top_locations, x="Location", y="Job Count", title="📍 Top Locations"), use_container_width=True)

    # --- MAP ---
    st.markdown("### 🗺️ Global Job Distribution Map")
    country_counts = df.groupby("Country").size().reset_index(name="Job Count")
    country_counts = country_counts[~country_counts["Country"].isin(["Unknown", "Remote"])]
    st.plotly_chart(px.scatter_geo(country_counts, locations="Country", locationmode="country names", size="Job Count", projection="natural earth"), use_container_width=True)

    # --- DETAILED TABLE ---
    st.markdown("### 📋 Detailed Lead Inventory")
    st.dataframe(df[["Job Title", "Company", "Location", "Country", "Type", "Posted", "Apply Link", "Company URL"]], use_container_width=True)

    # --- FILTERING SECTION ---
    st.divider()
    # st.markdown("### 🧹 Job List Cleanup Options")

    # st.session_state.show_old_jobs = st.toggle(
    #     " Include previously seen jobs in table & Excel",
    #     value=False,
    #     help="Turn ON to include jobs you've already seen in previous searches"
    # )
    if df.empty:
        st.warning(
            " No jobs found for this role and location.\n\n"
            " Try one of the following:\n"
            "- Change the job title or location\n"
            "- Broaden filters (timeline or job type)\n"
            "- Turn ON **Show old jobs** to view previously seen roles"
        )
    else:
        st.markdown("### 🎯 Lead Qualification Filters")

        f1, f2 = st.columns(2)

        with f1:
            revenue_q = st.selectbox(
                "💰 Company Revenue Range",
                [
                    "Any",
                    "$1M – $10M",
                    "$10M – $25M",
                    "$25M – $50M",
                    "$50M – $100M",
                    "$100M – $250M",
                    "$250M – $500M",
                    "$500M – $1B"
                ],
                index=0
            )

        with f2:
            company_size_q = st.selectbox(
                "👥 Company Employee Size",
                [
                    "Any",
                    "1–10",
                    "11–20",
                    "21–50",
                    "51–100",
                    "101–200",
                    "201–500",
                    "501–1000",
                    "1001–5000",
                    "5001–10000"
                ],
                index=0
            )
        st.markdown("### 🔍 Lead Filtering")
        # This button now works because the data is in session_state!
        qualification_threshold = st.number_input(
            "🏆 Qualification Threshold",
            min_value=0, 
            max_value=100, 
            value=25,   # Default set to 25
            step=1,     
            help="Companies with a Lead Score below this value will be excluded"
        )
        if st.button("🔍 Start Filtering"):
            st.session_state.show_leads = True

    def get_companies_from_results(company_df):
        return (
            company_df["Company"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )


    if st.session_state.show_leads:
        with st.spinner("🧠 Fetching company revenue & size intelligence..."):
            companies = get_companies_from_results(company_df)
            enrich_companies_from_list(companies)

        with st.spinner("📊 Recalculating lead scores..."):
            intel = load_company_intel(get_file_mtime(COMPANY_INTEL_FILE))


            scores = company_df.apply(
                lambda r: final_lead_score_salesforce(
                    r, intel, revenue_q, company_size_q
                ),
                axis=1,
                result_type="expand"
            )


            company_df["Lead Score"] = scores[0].astype(float)
            company_df["Rank (Breakout)"] = scores[1]

            company_df = company_df.sort_values("Lead Score", ascending=False)
            # update_structured_json_with_scores(company_df)
            update_structured_json_with_scores(company_df, job_q, loc_q)
        st.success("✅ Intelligence enrichment & ranking complete")

        st.dataframe(
            company_df[
                ["Company", "Countries", "Open_Roles", "Detected Need", "Why This Lead", "Lead Score", "Rank (Breakout)"]
            ],
            use_container_width=True
        )

        # if st.session_state.show_leads:
    #         # qualified_companies = get_high_score_companies(company_df, threshold=25)#15
    #         qualified_companies = get_high_score_companies(company_df, threshold=qualification_threshold)

    #         st.markdown("### 🧠 Deep Company Intelligence")
    #         st.write(f"Qualified Companies: {len(qualified_companies)}")

    #         if st.button("🚀 Generate Deep Company Reports"):
    #             progress_text = st.empty()
    #             progress_bar = st.progress(0)

    #             # -------------------------------
    #             # STEP 1: Deep Research
    #             # -------------------------------
    #             progress_text.text("🔍 Running deep company research...")
    #             progress_bar.progress(10)

    #             asyncio.run(run_deep_research_for_companies(qualified_companies))

    #             progress_bar.progress(50)

    #             # -------------------------------
    #             # STEP 2: Cleaning
    #             # -------------------------------
    #             # progress_text.text("🧹 Cleaning & structuring company intelligence...")
    #             # clean_all_unstructured_reports(
    #             #     unstructured_dir="Unstructured_data",
    #             #     structured_dir="structured_data"
    #             # )

    #             # progress_bar.progress(80)

    #             # update_structured_json_with_scores(company_df)

    #             # # -------------------------------
    #             # # STEP 3: Upload to Sheets
    #             # # -------------------------------
    #             # progress_text.text(" Uploading structured data to Google Sheets...")
    #             # upload_structured_folder_to_sheets()


    #             progress_text.text(" Cleaning & structuring company intelligence...")
    #             asyncio.run(clean_all_unstructured_reports_async(
    #                 unstructured_dir="Unstructured_data",
    #                 structured_dir="structured_data"
    #             ))

    #             progress_bar.progress(80)

    #             update_structured_json_with_scores(company_df, job_q, loc_q)

    #             # # -------------------------------
    #             # # STEP 3: Upload to Sheets
    #             # # -------------------------------
    #             # progress_text.text(" Uploading structured data to Google Sheets...")
    #             # #------------------------------------
    #             # struct_p = Path("structured_data")
    #             # all_struct_files = list(struct_p.glob("*_Structured.json"))
                
    #             # if all_struct_files:
    #             #     upload_batch_data(all_struct_files)
    #             # else:
    #             #     st.error(" No files found in structured_data folder.")

    #             # progress_bar.progress(70)
    #             # progress_text.text(" Generating AI Strategic Summaries...")
    #             # run_ai_strategic_layer()

    #             # # --- NEW CODE BLOCK STARTS HERE ---
    #             # progress_bar.progress(85)
    #             # progress_text.text(" Drafting Personalized Emails...")
                
    #             # try:
    #             #     run_email_generation_layer()
    #             #     st.success(" Emails generated and added to Google Sheets!")
    #             # except Exception as e:
    #             #     st.error(f" Email generation failed: {e}")
    #             # # --- NEW CODE BLOCK ENDS HERE ---

    #             # progress_bar.progress(100)
    #             # progress_text.text(" Pipeline completed successfully!")

    #             # st.success(" Deep research → structured data → Google Sheets upload completed!")

    #             # with st.spinner(" Generating AI Strategic Summaries..."):
    #             #     run_ai_strategic_layer()

    #             # st.success(" AI Strategic summaries added to Google Sheets")

    #             # -------------------------------
    #             # STEP 3: Upload to Sheets
    #             # -------------------------------
    #             progress_text.text(" Uploading structured data to Google Sheets...")
                
    #             struct_p = Path("structured_data")
    #             all_struct_files = list(struct_p.glob("*_Structured.json"))
                
    #             if all_struct_files:
    #                 upload_batch_data(all_struct_files)
    #             else:
    #                 st.error(" No files found in structured_data folder.")

    #             # Run AI Strategic Layer
    #             progress_bar.progress(70)
    #             progress_text.text(" Generating AI Strategic Summaries...")
    #             run_ai_strategic_layer()

    #             # ---------------------------------------------------------
    #             # 👇 NEW LOGIC: ENRICHMENT & STOP 👇
    #             # ---------------------------------------------------------
                
    #             progress_bar.progress(85)
    #             progress_text.text("🧠 Extracting CEO & Contact Info...")
                
    #             # Run the enrichment script to create the CSV for verification
    #             csv_path = asyncio.run(run_data_enrichment())
                
    #             progress_bar.progress(100)

    #             if csv_path and "Failed" not in csv_path:
    #                 st.success(" Phase 1 Complete! Data available for verification below.")
    #                 st.balloons()
                    
    #                 # 🔒 LOCK THE STATE: This makes the Download Button appear in the next section
    #                 st.session_state.enrichment_ready = True
    #                 st.session_state.enrichment_csv = csv_path
    #             else:
    #                 st.error(" Data Enrichment Failed. Check logs.")

    #             # ---------------------------------------------------------
    #             # 👆 PIPELINE STOPS HERE FOR USER INPUT 👆
    #             # ---------------------------------------------------------





    # # --- DOWNLOAD ---
    # csv = df.to_csv(index=False).encode("utf-8")
    # st.download_button(label=" Download Full Report (CSV)", data=csv, file_name=f"Report.csv", mime="text/csv")





    #     if st.session_state.show_leads:
    #     # qualified_companies = get_high_score_companies(company_df, threshold=25)#15
    #         qualified_companies = get_high_score_companies(company_df, threshold=qualification_threshold)

    #         st.markdown("### 🧠 Deep Company Intelligence")
    #         st.write(f"Qualified Companies: {len(qualified_companies)}")

    #         # --- MAIN PROCESS BUTTON ---
    #         if st.button("🚀 Generate Deep Company Reports"):
    #             progress_text = st.empty()
    #             progress_bar = st.progress(0)

    #             # -------------------------------
    #             # STEP 1: Deep Research
    #             # -------------------------------
    #             progress_text.text("🔍 Running deep company research...")
    #             progress_bar.progress(10)

    #             asyncio.run(run_deep_research_for_companies(qualified_companies))

    #             progress_bar.progress(40)

    #             # -------------------------------
    #             # STEP 2: Cleaning & Structuring
    #             # -------------------------------
    #             progress_text.text("🧹 Cleaning & structuring company intelligence...")
    #             asyncio.run(clean_all_unstructured_reports_async(
    #                 unstructured_dir="Unstructured_data",
    #                 structured_dir="structured_data"
    #             ))

    #             progress_bar.progress(60)

    #             # Update JSONs with Streamlit scores
    #             update_structured_json_with_scores(company_df, job_q, loc_q)

    #             # -------------------------------
    #             # STEP 3: Upload to Sheets & AI Summary
    #             # -------------------------------
    #             progress_text.text("📤 Uploading structured data to Google Sheets...")
                
    #             struct_p = Path("structured_data")
    #             all_struct_files = list(struct_p.glob("*_Structured.json"))
                
    #             if all_struct_files:
    #                 upload_batch_data(all_struct_files)
    #             else:
    #                 st.error("⚠️ No files found in structured_data folder.")

    #             # Run AI Strategic Layer
    #             progress_bar.progress(70)
    #             progress_text.text("🤖 Generating AI Strategic Summaries...")
    #             run_ai_strategic_layer()

    #             # ---------------------------------------------------------
    #             # 👇 NEW LOGIC: ENRICHMENT & STOP 👇
    #             # ---------------------------------------------------------
                
    #             progress_bar.progress(85)
    #             progress_text.text("🧠 Extracting CEO & Contact Info via Groq...")
                
    #             # Run the enrichment script to create the CSV for verification
    #             csv_path = asyncio.run(run_data_enrichment())
                
    #             progress_bar.progress(100)

    #             if csv_path and "Failed" not in csv_path:
    #                 st.success("✅ Phase 1 Complete! Data available for verification below.")
    #                 # st.balloons()
                    
    #                 # 🔒 LOCK THE STATE: This triggers the Download Button to appear below
    #                 st.session_state.enrichment_ready = True
    #                 st.session_state.enrichment_csv = csv_path
    #             else:
    #                 st.error("❌ Data Enrichment Failed. Check logs.")

    #             # ---------------------------------------------------------
    #             # 👆 PIPELINE STOPS HERE FOR USER INPUT 👆
    #             # ---------------------------------------------------------


    #         # ---------------------------------------------------------
    #         # 👇 NEW SECTION: STEP 4 (Verify & Generate Emails) 👇
    #         # ---------------------------------------------------------
    #         st.divider()
    #         st.markdown("## 📧 Step 4: Verify & Generate Emails (Human-in-the-Loop)")

    #         # A. SHOW DOWNLOAD BUTTON (If Phase 1 is done)
    #         if st.session_state.get("enrichment_ready"):
    #             st.info("👇 **Action Required:** Download this file, add 'Email_ID', verify CEO names, and re-upload.")
                
    #             # Load CSV to show preview
    #             try:
    #                 if st.session_state.enrichment_csv and os.path.exists(st.session_state.enrichment_csv):
    #                     enrich_df = pd.read_csv(st.session_state.enrichment_csv)
    #                     st.dataframe(enrich_df.head(), use_container_width=True)
                        
    #                     with open(st.session_state.enrichment_csv, "rb") as f:
    #                         st.download_button(
    #                             label="📥 Download Enriched Data (CSV)",
    #                             data=f,
    #                             file_name="Enriched_Leads_For_Verification.csv",
    #                             mime="text/csv"
    #                         )
    #                 else:
    #                     st.warning("⚠️ CSV file not found. Please re-run Phase 1.")
    #             except Exception as e:
    #                 st.warning(f"Could not read generated CSV: {e}")

    #         # B. UPLOAD VERIFIED FILE & GENERATE EMAILS
    #         st.markdown("### 📤 Upload Verified Data to Start Emailing")
    #         verified_file = st.file_uploader("Upload the CSV with filled 'Email_ID' column", type=["csv"], key="email_uploader")

    #         if verified_file:
    #             if st.button("🚀 Sync Data & Generate Emails"):
    #                 progress_text = st.empty()
    #                 progress_bar = st.progress(0)
                    
    #                 try:
    #                     # 1. Read File
    #                     verified_df = pd.read_csv(verified_file)
                        
    #                     # 2. Check columns
    #                     cols = [c.strip() for c in verified_df.columns]
    #                     if "Email_ID" not in cols:
    #                         st.error("❌ File must have an 'Email_ID' column.")
    #                         st.stop()
                        
    #                     # 3. Update Google Sheet
    #                     progress_text.text("🔄 Syncing verified data to Google Sheets...")
    #                     progress_bar.progress(30)
                        
    #                     updated_count = update_sheet_with_enriched_data(verified_df)
                        
    #                     if updated_count > 0:
    #                         st.success(f"✅ Updated {updated_count} rows in Google Sheets!")
    #                     else:
    #                         st.warning("⚠️ No rows updated (Check if Company Names match Google Sheet). Proceeding...")
                        
    #                     # 4. Generate Emails
    #                     progress_text.text("✍️ AI is writing personalized emails...")
    #                     progress_bar.progress(60)
                        
    #                     run_email_generation_layer()
                        
    #                     progress_bar.progress(100)
    #                     st.success("🎉 Process Complete! Emails have been drafted in the Google Sheet.")
    #                     # st.balloons()

    #                     # 2. Direct Authority aur Link ki guide dikhayein
    #                     st.info(" Note: Make sure your email has access to this sheet.")

    #                     # 3. Final Button
    #                     st.link_button(
    #                         label="📊 Open Google Sheet", 
    #                         url="https://docs.google.com/spreadsheets/d/1yYKCYrILgvSrjeUObh2iE34tBie-Bkh6IIBAKiuaDRc/edit?gid=0#gid=0",
    #                         type="primary"
    #                     )
                        
    #                 except Exception as e:
    #                     st.error(f"❌ An error occurred: {e}")

    #         st.divider()

    # # --- ORIGINAL DOWNLOAD BUTTON ---
    # csv = df.to_csv(index=False).encode("utf-8")
    # st.download_button(label=" Download Full Report (CSV)", data=csv, file_name=f"Report.csv", mime="text/csv")
         

        # if st.session_state.show_leads:
    #     # qualified_companies = get_high_score_companies(company_df, threshold=25)#15
    #         qualified_companies = get_high_score_companies(company_df, threshold=qualification_threshold)
 
    #         st.markdown("### 🧠 Deep Company Intelligence")
    #         st.write(f"Qualified Companies: {len(qualified_companies)}")
 
    #         # --- MAIN PROCESS BUTTON ---
    #         if st.button("🚀 Generate Deep Company Reports"):
    #             progress_text = st.empty()
    #             progress_bar = st.progress(0)
 
    #             # -------------------------------
    #             # STEP 1: Deep Research
    #             # -------------------------------
    #             progress_text.text("🔍 Running deep company research...")
    #             progress_bar.progress(10)
 
    #             asyncio.run(run_deep_research_for_companies(qualified_companies))
 
    #             progress_bar.progress(40)
 
    #             # -------------------------------
    #             # STEP 2: Cleaning & Structuring
    #             # -------------------------------
    #             progress_text.text("🧹 Cleaning & structuring company intelligence...")
    #             asyncio.run(clean_all_unstructured_reports_async(
    #                 unstructured_dir="Unstructured_data",
    #                 structured_dir="structured_data"
    #             ))
 
    #             progress_bar.progress(60)
 
    #             # Update JSONs with Streamlit scores
    #             update_structured_json_with_scores(company_df, job_q, loc_q)
 
    #             # -------------------------------
    #             # STEP 3: Upload to Sheets & AI Summary
    #             # -------------------------------
    #             progress_text.text("📤 Uploading structured data to Google Sheets...")
               
    #             struct_p = Path("structured_data")
    #             all_struct_files = list(struct_p.glob("*_Structured.json"))
               
    #             if all_struct_files:
    #                 upload_batch_data(all_struct_files)
    #             else:
    #                 st.error("⚠️ No files found in structured_data folder.")
 
    #             # Run AI Strategic Layer
    #             progress_bar.progress(70)
    #             progress_text.text("🤖 Generating AI Strategic Summaries...")
    #             run_ai_strategic_layer()
 
    #             # ---------------------------------------------------------
    #             # 👇 NEW LOGIC: ENRICHMENT & STOP 👇
    #             # ---------------------------------------------------------
               
    #             progress_bar.progress(85)
    #             progress_text.text("🧠 Extracting CEO & Contact Info via Groq...")
               
    #             # Run the enrichment script to create the CSV for verification
    #             csv_path = asyncio.run(run_data_enrichment())
               
    #             progress_bar.progress(100)
 
    #             if csv_path and "Failed" not in csv_path:
    #                 st.success("✅ Phase 1 Complete! Data available for verification below.")
    #                 # st.balloons()
                   
    #                 # 🔒 LOCK THE STATE: This triggers the Download Button to appear below
    #                 st.session_state.enrichment_ready = True
    #                 st.session_state.enrichment_csv = csv_path
    #             else:
    #                 st.error("❌ Data Enrichment Failed. Check logs.")
 
    #             # ---------------------------------------------------------
    #             # 👆 PIPELINE STOPS HERE FOR USER INPUT 👆
    #             # ---------------------------------------------------------
 
 
    #         # ---------------------------------------------------------
    #         # 👇 NEW SECTION: STEP 4 (Verify & Generate Emails) 👇
    #         # ---------------------------------------------------------
    #         st.divider()
    #         st.markdown("## 📧 Step 4: Verify & Generate Emails (Human-in-the-Loop)")
 
    #         # A. SHOW DOWNLOAD BUTTON (If Phase 1 is done)
    #         if st.session_state.get("enrichment_ready"):
    #             st.info("👇 **Action Required:** Download this file, add 'Email_ID', verify CEO names, and re-upload.")
               
    #             # Load CSV to show preview
    #             try:
    #                 if st.session_state.enrichment_csv and os.path.exists(st.session_state.enrichment_csv):
    #                     enrich_df = pd.read_csv(st.session_state.enrichment_csv)
    #                     st.dataframe(enrich_df.head(), use_container_width=True)
                       
    #                     with open(st.session_state.enrichment_csv, "rb") as f:
    #                         st.download_button(
    #                             label="📥 Download Enriched Data (CSV)",
    #                             data=f,
    #                             file_name="Enriched_Leads_For_Verification.csv",
    #                             mime="text/csv"
    #                         )
    #                 else:
    #                     st.warning("⚠️ CSV file not found. Please re-run Phase 1.")
    #             except Exception as e:
    #                 st.warning(f"Could not read generated CSV: {e}")
 
    #         # B. UPLOAD VERIFIED FILE & GENERATE EMAILS
    #         st.markdown("### 📤 Upload Verified Data to Start Emailing")
    #         verified_file = st.file_uploader("Upload the CSV with filled 'Email_ID' column", type=["csv"], key="email_uploader")
 
    #         if verified_file:
    #             if st.button("🚀 Sync Data & Generate Emails"):
    #                 progress_text = st.empty()
    #                 progress_bar = st.progress(0)
                   
    #                 try:
    #                     # 1. Read File
    #                     verified_df = pd.read_csv(verified_file)
                       
    #                     # 2. Check columns
    #                     cols = [c.strip() for c in verified_df.columns]
    #                     if "Email_ID" not in cols:
    #                         st.error("❌ File must have an 'Email_ID' column.")
    #                         st.stop()
                       
    #                     # 3. Update Google Sheet
    #                     progress_text.text("🔄 Syncing verified data to Google Sheets...")
    #                     progress_bar.progress(30)
                       
    #                     updated_count = update_sheet_with_enriched_data(verified_df)
                       
    #                     if updated_count > 0:
    #                         st.success(f"✅ Updated {updated_count} rows in Google Sheets!")
    #                     else:
    #                         st.warning("⚠️ No rows updated (Check if Company Names match Google Sheet). Proceeding...")
                       
    #                     # 4. Generate Emails
    #                     progress_text.text("✍️ AI is writing personalized emails...")
    #                     progress_bar.progress(60)
                       
    #                     run_email_generation_layer()
                       
    #                     progress_bar.progress(80)
    #                     st.success("🎉 Process Complete! Emails have been drafted in the Google Sheet.")
    #                     # st.balloons()
    #                     leads = read_leads_from_sheet(GOOGLE_SHEET_NAME)
    #                     result = send_to_instantly(leads)
    #                     progress_bar.progress(100)
    #                     st.session_state.instantly_ready = True
    #                     st.session_state.instantly_result = result
    #                     st.success("🚀 Leads sent to Instantly for outreach!")
 
    #                     st.success(
    #                         f"🎉 Instantly Campaign Updated!\n\n"
    #                         f"• Total sent: {result['total_sent']}\n"
    #                         f"• Uploaded: {result['leads_uploaded']}\n"
    #                         f"• Skipped: {result['skipped_count']}"
    #                     )
                        
    #                     st.divider()
                       
    #                     st.info(" Note: Make sure your email has access to this sheet.")

    #                     # 3. Final Button
    #                     st.link_button(
    #                         label="📊 Open Google Sheet", 
    #                         url="https://docs.google.com/spreadsheets/d/1yYKCYrILgvSrjeUObh2iE34tBie-Bkh6IIBAKiuaDRc/edit?gid=0#gid=0",
    #                         type="primary"
    #                     )
                       
    #                 except Exception as e:
    #                     st.error(f"❌ An error occurred: {e}")
    #         # ================= INSTANTLY CAMPAIGN CONTROL =================
 
    #         if st.session_state.get("instantly_ready"):
 
    #             st.divider()
    #             st.markdown("## 🚀 Instantly Campaign Control")
 
    #             if st.session_state.get("instantly_activated"):
    #                 st.success("🟢 Campaign is already active")
    #             else:
    #                 if st.button("▶️ Activate Instantly Campaign"):
    #                     with st.spinner("Activating campaign..."):
    #                         try:
    #                             campaign = activate_campaign(INSTANTLY_CAMPAIGN_ID)
 
    #                             if campaign.get("status") == 1:
    #                                 st.session_state.instantly_activated = True
    #                                 st.success("✅ Campaign activated successfully!")
    #                             else:
    #                                 st.warning(
    #                                     f"⚠️ Campaign responded but status = {campaign.get('status')}"
    #                                 )
 
    #                         except Exception as e:
    #                             st.error(f"❌ Failed to activate campaign: {e}")
 
 
 
   
    # # --- ORIGINAL DOWNLOAD BUTTON ---
    # csv = df.to_csv(index=False).encode("utf-8")
    # st.download_button(label=" Download Full Report (CSV)", data=csv, file_name=f"Report.csv", mime="text/csv")


    #     if st.session_state.show_leads:
    #         # qualified_companies = get_high_score_companies(company_df, threshold=25)#15
    #         qualified_companies = get_high_score_companies(company_df, threshold=qualification_threshold)
 
    #         st.markdown("### 🧠 Deep Company Intelligence")
    #         st.write(f"Qualified Companies: {len(qualified_companies)}")
 
    #         # --- MAIN PROCESS BUTTON ---
    #         if st.button("🚀 Generate Deep Company Reports"):
    #             progress_text = st.empty()
    #             progress_bar = st.progress(0)
 
    #             # -------------------------------
    #             # STEP 1: Deep Research
    #             # -------------------------------
    #             progress_text.text("🔍 Running deep company research...")
    #             progress_bar.progress(10)
 
    #             asyncio.run(run_deep_research_for_companies(qualified_companies))
 
    #             progress_bar.progress(40)
 
    #             # -------------------------------
    #             # STEP 2: Cleaning & Structuring
    #             # -------------------------------
    #             progress_text.text("🧹 Cleaning & structuring company intelligence...")
    #             asyncio.run(clean_all_unstructured_reports_async(
    #                 unstructured_dir="Unstructured_data",
    #                 structured_dir="structured_data"
    #             ))
 
    #             progress_bar.progress(60)
 
    #             # Update JSONs with Streamlit scores
    #             update_structured_json_with_scores(company_df, job_q, loc_q)
 
    #             # -------------------------------
    #             # STEP 3: Upload to Sheets & AI Summary
    #             # -------------------------------
    #             progress_text.text("📤 Uploading structured data to Google Sheets...")
               
    #             struct_p = Path("structured_data")
    #             all_struct_files = list(struct_p.glob("*_Structured.json"))
               
    #             if all_struct_files:
    #                 upload_batch_data(all_struct_files)
    #             else:
    #                 st.error("⚠️ No files found in structured_data folder.")
 
    #             # Run AI Strategic Layer
    #             progress_bar.progress(70)
    #             progress_text.text("🤖 Generating AI Strategic Summaries...")
    #             run_ai_strategic_layer()
 
    #             # ---------------------------------------------------------
    #             # 👇 NEW LOGIC: ENRICHMENT & STOP 👇
    #             # ---------------------------------------------------------
               
    #             progress_bar.progress(85)
    #             progress_text.text("🧠 Extracting CEO & Contact Info via Groq...")
               
    #             # Run the enrichment script to create the CSV for verification
    #             csv_path = asyncio.run(run_data_enrichment())
               
    #             progress_bar.progress(100)
 
    #             if csv_path and "Failed" not in csv_path:
    #                 st.success("✅ Phase 1 Complete! Data available for verification below.")
    #                 # st.balloons()
                   
    #                 # 🔒 LOCK THE STATE: This triggers the Download Button to appear below
    #                 st.session_state.enrichment_ready = True
    #                 st.session_state.enrichment_csv = csv_path
    #             else:
    #                 st.error("❌ Data Enrichment Failed. Check logs.")
 
    #             # ---------------------------------------------------------
    #             # 👆 PIPELINE STOPS HERE FOR USER INPUT 👆
    #             # ---------------------------------------------------------
 
 
    #         # ---------------------------------------------------------
    #         # 👇 NEW SECTION: STEP 4 (Verify & Generate Emails) 👇
    #         # ---------------------------------------------------------
    #         st.divider()
    #         st.markdown("## 📧 Step 4: Verify & Generate Emails (Human-in-the-Loop)")
 
    #         # A. SHOW DOWNLOAD BUTTON (If Phase 1 is done)
    #         if st.session_state.get("enrichment_ready"):
    #             st.info("👇 **Action Required:** Download this file, add 'Email_ID', verify CEO names, and re-upload.")
               
    #             # Load CSV to show preview
    #             try:
    #                 if st.session_state.enrichment_csv and os.path.exists(st.session_state.enrichment_csv):
    #                     enrich_df = pd.read_csv(st.session_state.enrichment_csv)
    #                     st.dataframe(enrich_df.head(), use_container_width=True)
                       
    #                     with open(st.session_state.enrichment_csv, "rb") as f:
    #                         st.download_button(
    #                             label="📥 Download Enriched Data (CSV)",
    #                             data=f,
    #                             file_name="Enriched_Leads_For_Verification.csv",
    #                             mime="text/csv"
    #                         )
    #                 else:
    #                     st.warning("⚠️ CSV file not found. Please re-run Phase 1.")
    #             except Exception as e:
    #                 st.warning(f"Could not read generated CSV: {e}")
 
    #         # B. UPLOAD VERIFIED FILE & GENERATE EMAILS
    #         st.markdown("### 📤 Upload Verified Data to Start Emailing")
    #         verified_file = st.file_uploader("Upload the CSV with filled 'Email_ID' column", type=["csv"], key="email_uploader")
 
    #         if verified_file:
    #             if st.button("🚀 Sync Data & Generate Emails"):
    #                 progress_text = st.empty()
    #                 progress_bar = st.progress(0)
                   
    #                 try:
    #                     # 1. Read File
    #                     verified_df = pd.read_csv(verified_file)
                       
    #                     # 2. Check columns
    #                     cols = [c.strip() for c in verified_df.columns]
    #                     if "Email_ID" not in cols:
    #                         st.error("❌ File must have an 'Email_ID' column.")
    #                         st.stop()
                       
    #                     # 3. Update Google Sheet
    #                     progress_text.text("🔄 Syncing verified data to Google Sheets...")
    #                     progress_bar.progress(30)
                       
    #                     updated_count = update_sheet_with_enriched_data(verified_df)
                       
    #                     if updated_count > 0:
    #                         st.success(f"✅ Updated {updated_count} rows in Google Sheets!")
    #                     else:
    #                         st.warning("⚠️ No rows updated (Check if Company Names match Google Sheet). Proceeding...")
                       
    #                     # 4. Generate Emails
    #                     progress_text.text("✍️ AI is writing personalized emails...")
    #                     progress_bar.progress(60)
                       
    #                     run_email_generation_layer()
                       
    #                     progress_bar.progress(90)
    #                     st.success("🎉 Process Complete! Emails have been drafted in the Google Sheet.")
                        
    #                     # --- REMOVED INSTANTLY AUTOMATION ---
    #                     # Instead, fetching data for manual download
                        
    #                     progress_text.text("📥 Fetching final data for download...")
                        
    #                     # Fetch data back from Google Sheet to ensure we have the generated emails
    #                     gc_final = connect_to_sheet()
    #                     sh_final = gc_final.open(GOOGLE_SHEET_NAME)
    #                     ws_final = sh_final.sheet1
    #                     all_data = ws_final.get_all_records()
                        
    #                     final_df = pd.DataFrame(all_data)

    #                     # Define required columns for Instantly
    #                     # Using 'Company' as fallback for 'meta_company_name' if needed
    #                     if "meta_company_name" not in final_df.columns and "Company" in final_df.columns:
    #                         final_df.rename(columns={"Company": "meta_company_name"}, inplace=True)
                            
    #                     target_cols = ["meta_company_name", "Email Subject", "Email Body", "CEO Name", "Email ID"]
                        
    #                     # Filter to keep only available target columns
    #                     available_cols = [c for c in target_cols if c in final_df.columns]
    #                     final_output = final_df[available_cols]

    #                     progress_bar.progress(100)
                        
    #                     # Show Success and Download Button
    #                     st.success("🚀 Ready for Instantly! Download the file below.")
                        
    #                     st.divider()
                        
    #                     csv_final = final_output.to_csv(index=False).encode("utf-8")
    #                     st.download_button(
    #                         label="📥 Download Final Leads (For Instantly)", 
    #                         data=csv_final, 
    #                         file_name="Final_Instantly_Leads.csv", 
    #                         mime="text/csv"
    #                     )

    #                     st.info(" Note: Make sure your email has access to this sheet.")

    #                     # 3. Final Button
    #                     st.link_button(
    #                         label="📊 Open Google Sheet", 
    #                         url="https://docs.google.com/spreadsheets/d/1yYKCYrILgvSrjeUObh2iE34tBie-Bkh6IIBAKiuaDRc/edit?gid=0#gid=0",
    #                         type="primary"
    #                     )
                       
    #                 except Exception as e:
    #                     st.error(f"❌ An error occurred: {e}")

    # # --- ORIGINAL DOWNLOAD BUTTON ---
    # if st.session_state.df is not None:
    #     csv = st.session_state.df.to_csv(index=False).encode("utf-8")
    #     st.download_button(label=" Download Full Report (CSV)", data=csv, file_name=f"Report.csv", mime="text/csv")

        # ---------------------------------------------------------
            # 👇 NEW SECTION: STEP 4 (Verify & Generate Emails) 👇
            # ---------------------------------------------------------
        # st.divider()
        # st.markdown("## 📧 Step 4: Verify & Generate Emails (Human-in-the-Loop)")

#         # A. SHOW DOWNLOAD BUTTON (If Phase 1 is done)
#         if st.session_state.get("enrichment_ready"):
#             st.info("👇 **Action Required:** Download this file, add 'Email_ID', verify CEO names, and re-upload.")
            
#             # Load CSV to show preview
#             try:
#                 if st.session_state.enrichment_csv and os.path.exists(st.session_state.enrichment_csv):
#                     enrich_df = pd.read_csv(st.session_state.enrichment_csv)
#                     st.dataframe(enrich_df.head(), use_container_width=True)
                    
#                     with open(st.session_state.enrichment_csv, "rb") as f:
#                         st.download_button(
#                             label="📥 Download Enriched Data (CSV)",
#                             data=f,
#                             file_name="Enriched_Leads_For_Verification.csv",
#                             mime="text/csv"
#                         )
#                 else:
#                     st.warning("⚠️ CSV file not found. Please re-run Phase 1.")
#             except Exception as e:
#                 st.warning(f"Could not read generated CSV: {e}")

#         # B. UPLOAD VERIFIED FILE & GENERATE EMAILS
#         st.markdown("### 📤 Upload Verified Data to Start Emailing")
#         verified_file = st.file_uploader("Upload the CSV with filled 'Email_ID' column", type=["csv"], key="email_uploader")

#         if verified_file:
#             # Trigger processing on button click
#             if st.button("🚀 Sync Data & Generate Emails"):
#                 st.session_state.processing_emails = True
            
#             # Main Logic inside Session State check to prevent reset during interactions
#             if st.session_state.get("processing_emails"):
#                 progress_text = st.empty()
#                 progress_bar = st.progress(0)
                
#                 try:
#                     # 1. Read File
#                     # Reset file pointer to the beginning to avoid read errors
#                     verified_file.seek(0)
#                     verified_df = pd.read_csv(verified_file)
                    
#                     # 2. Check columns
#                     cols = [c.strip() for c in verified_df.columns]
#                     if "Email_ID" not in cols:
#                         st.error("❌ File must have an 'Email_ID' column.")
#                         st.stop()

#                     # -----------------------------------------------------
#                     # SAVE CURRENT COMPANY NAMES TO FILTER LATER
#                     # -----------------------------------------------------
#                     # We capture the list of companies in this specific batch.
#                     # This allows us to filter the Google Sheet data later so we don't get old leads.
#                     current_batch_companies = []
#                     if "meta_company_name" in verified_df.columns:
#                         current_batch_companies = verified_df["meta_company_name"].astype(str).tolist()
#                     elif "Company" in verified_df.columns:
#                         current_batch_companies = verified_df["Company"].astype(str).tolist()
                    
#                     # 3. Update Google Sheet
#                     progress_text.text("🔄 Syncing verified data to Google Sheets...")
#                     progress_bar.progress(30)
                    
#                     updated_count = update_sheet_with_enriched_data(verified_df)
                    
#                     if updated_count > 0:
#                         st.success(f"✅ Updated {updated_count} rows in Google Sheets!")
#                     else:
#                         st.warning("⚠️ No rows updated (Check if Company Names match Google Sheet). Proceeding...")
                    
#                     # 4. Generate Emails
#                     progress_text.text("✍️ AI is writing personalized emails...")
#                     progress_bar.progress(60)
                    
#                     run_email_generation_layer()
                    
#                     progress_bar.progress(90)
#                     st.success("🎉 Process Complete! Emails have been drafted in the Google Sheet.")
                    
#                     # ---------------------------------------------------------
#                     # 👇 NEW LOGIC: FETCH ONLY CURRENT BATCH DATA 👇
#                     # ---------------------------------------------------------
#                     progress_text.text("📥 Fetching final data for download...")
                    
#                     # Fetch ALL data from Sheet (this includes old and new data)
#                     gc_final = connect_to_sheet()
#                     sh_final = gc_final.open(GOOGLE_SHEET_NAME)
#                     ws_final = sh_final.sheet1
#                     all_data = ws_final.get_all_records()
                    
#                     full_df = pd.DataFrame(all_data)

#                     # Handle Column Name Variations (Normalize 'Company' to 'meta_company_name')
#                     if "meta_company_name" not in full_df.columns and "Company" in full_df.columns:
#                         full_df.rename(columns={"Company": "meta_company_name"}, inplace=True)

#                     # --- CRITICAL FILTERING STEP ---
#                     # Filter to keep ONLY rows that match the companies in our uploaded file
#                     if current_batch_companies:
#                         # Normalize names for better matching (strip spaces, lowercase)
#                         batch_set = set(str(x).strip().lower() for x in current_batch_companies)
                        
#                         # Apply the filter
#                         final_df = full_df[full_df["meta_company_name"].astype(str).apply(lambda x: x.strip().lower()).isin(batch_set)]
#                     else:
#                         # Fallback: If no company names were found (rare), keep full data
#                         final_df = full_df

#                     # Keep only required columns for Instantly
#                     target_cols = ["meta_company_name", "Email Subject", "Email Body", "CEO Name", "Email ID"]
#                     available_cols = [c for c in target_cols if c in final_df.columns]
#                     final_output = final_df[available_cols]

#                     progress_bar.progress(100)
                    
#                     # --- SAVE TO SESSION STATE TO PERSIST AFTER DOWNLOAD CLICK ---
#                     # This ensures the download button remains visible even if the app reruns
#                     st.session_state.final_csv_ready = final_output.to_csv(index=False).encode("utf-8")
                    
#                     # Turn off the processing flag so we don't re-run logic unnecessarily
#                     st.session_state.processing_emails = False 
                    
#                 except Exception as e:
#                     st.error(f"❌ An error occurred: {e}")
#                     st.session_state.processing_emails = False

#         # ---------------------------------------------------------
#         # SHOW DOWNLOAD BUTTON (Persistent)
#         # ---------------------------------------------------------
#         # This block is outside the button logic, so it stays visible
#         if st.session_state.get("final_csv_ready"):
#             st.divider()
#             st.success("🚀 Ready for Instantly! Download only the NEW leads below.")
            
#             st.download_button(
#                 label="📥 Download Final Leads (Only Current Batch)", 
#                 data=st.session_state.final_csv_ready, 
#                 file_name="Final_Instantly_Leads.csv", 
#                 mime="text/csv",
#                 key="final_download_btn_persistent"
#             )

#             st.info(" Note: Make sure your email has access to this sheet.")

#             # Final Link Button
#             st.link_button(
#                 label="📊 Open Google Sheet", 
#                 url="https://docs.google.com/spreadsheets/d/1yYKCYrILgvSrjeUObh2iE34tBie-Bkh6IIBAKiuaDRc/edit?gid=0#gid=0",
#                 type="primary"
#             )

# # --- ORIGINAL REPORT DOWNLOAD BUTTON (Outside the loop) ---
# if st.session_state.df is not None:
#     csv = st.session_state.df.to_csv(index=False).encode("utf-8")
#     st.download_button(label=" Download Full Report (CSV)", data=csv, file_name=f"Report.csv", mime="text/csv")

        # if st.session_state.show_leads:
        #     # qualified_companies = get_high_score_companies(company_df, threshold=25)#15
        #     qualified_companies = get_high_score_companies(company_df, threshold=qualification_threshold)

        #     st.markdown("### 🧠 Deep Company Intelligence")
        #     st.write(f"Qualified Companies: {len(qualified_companies)}")

        #     # --- MAIN PROCESS BUTTON ---
        #     if st.button("🚀 Generate Deep Company Reports"):
        #         progress_text = st.empty()
        #         progress_bar = st.progress(0)

        #         # -------------------------------
        #         # STEP 1: Deep Research
        #         # -------------------------------
        #         progress_text.text("🔍 Running deep company research...")
        #         progress_bar.progress(10)

        #         asyncio.run(run_deep_research_for_companies(qualified_companies))

        #         progress_bar.progress(40)

        #         # -------------------------------
        #         # STEP 2: Cleaning & Structuring
        #         # -------------------------------
        #         progress_text.text("🧹 Cleaning & structuring company intelligence...")
        #         asyncio.run(clean_all_unstructured_reports_async(
        #             unstructured_dir="Unstructured_data",
        #             structured_dir="structured_data"
        #         ))

        #         progress_bar.progress(60)

        #         # Update JSONs with Streamlit scores
        #         update_structured_json_with_scores(company_df, job_q, loc_q)

        #         # -------------------------------
        #         # STEP 3: Upload to Sheets & AI Summary
        #         # -------------------------------
        #         progress_text.text("📤 Uploading structured data to Google Sheets...")
                
        #         struct_p = Path("structured_data")
        #         all_struct_files = list(struct_p.glob("*_Structured.json"))
                
        #         if all_struct_files:
        #             upload_batch_data(all_struct_files)
        #         else:
        #             st.error("⚠️ No files found in structured_data folder.")

        #         # Run AI Strategic Layer
        #         progress_bar.progress(70)
        #         progress_text.text("🤖 Generating AI Strategic Summaries...")
        #         run_ai_strategic_layer()

        #         # ---------------------------------------------------------
        #         # 👇 NEW LOGIC: ENRICHMENT & STOP 👇
        #         # ---------------------------------------------------------
                
        #         progress_bar.progress(85)
        #         progress_text.text("🧠 Extracting CEO & Contact Info via Groq...")
                
        #         # Run the enrichment script to create the CSV for verification
        #         csv_path = asyncio.run(run_data_enrichment())
                
        #         progress_bar.progress(100)

        #         if csv_path and "Failed" not in csv_path:
        #             st.success("✅ Phase 1 Complete! Data available for verification below.")
        #             # st.balloons()
                    
        #             # 🔒 LOCK THE STATE: This triggers the Download Button to appear below
        #             st.session_state.enrichment_ready = True
        #             st.session_state.enrichment_csv = csv_path
        #         else:
        #             st.error("❌ Data Enrichment Failed. Check logs.")

        #         # ---------------------------------------------------------
        #         # 👆 PIPELINE STOPS HERE FOR USER INPUT 👆
        #         # ---------------------------------------------------------


        #     # ---------------------------------------------------------
        #     # 👇 NEW SECTION: STEP 4 (Verify & Generate Emails) 👇
        #     # ---------------------------------------------------------
        #     st.divider()
        #     st.markdown("## 📧 Step 4: Verify & Generate Emails (Human-in-the-Loop)")

        #     # A. SHOW DOWNLOAD BUTTON (If Phase 1 is done)
        #     if st.session_state.get("enrichment_ready"):
        #         st.info("👇 **Action Required:** Download this file, add 'Email_ID', verify CEO names, and re-upload.")
                
        #         # Load CSV to show preview
        #         try:
        #             if st.session_state.enrichment_csv and os.path.exists(st.session_state.enrichment_csv):
        #                 enrich_df = pd.read_csv(st.session_state.enrichment_csv)
        #                 st.dataframe(enrich_df.head(), use_container_width=True)
                        
        #                 with open(st.session_state.enrichment_csv, "rb") as f:
        #                     st.download_button(
        #                         label="📥 Download Enriched Data (CSV)",
        #                         data=f,
        #                         file_name="Enriched_Leads_For_Verification.csv",
        #                         mime="text/csv"
        #                     )
        #             else:
        #                 st.warning("⚠️ CSV file not found. Please re-run Phase 1.")
        #         except Exception as e:
        #             st.warning(f"Could not read generated CSV: {e}")

        #     # B. UPLOAD VERIFIED FILE & GENERATE EMAILS
        #     st.markdown("### 📤 Upload Verified Data to Start Emailing")
        #     verified_file = st.file_uploader("Upload the CSV with filled 'Email_ID' column", type=["csv"], key="email_uploader")

        #     if verified_file:
        #         # Trigger processing on button click
        #         if st.button("🚀 Sync Data & Generate Emails"):
        #             st.session_state.processing_emails = True
                
        #         # Main Logic inside Session State check to prevent reset during interactions
        #         if st.session_state.get("processing_emails"):
        #             progress_text = st.empty()
        #             progress_bar = st.progress(0)
                    
        #             try:
        #                 # 1. Read File
        #                 # Reset file pointer to the beginning to avoid read errors
        #                 verified_file.seek(0)
        #                 verified_df = pd.read_csv(verified_file)
                        
        #                 # 2. Check columns
        #                 cols = [c.strip() for c in verified_df.columns]
        #                 if "Email_ID" not in cols:
        #                     st.error("❌ File must have an 'Email_ID' column.")
        #                     st.stop()

        #                 # -----------------------------------------------------
        #                 # SAVE CURRENT COMPANY NAMES TO FILTER LATER
        #                 # -----------------------------------------------------
        #                 # We capture the list of companies in this specific batch.
        #                 # This allows us to filter the Google Sheet data later so we don't get old leads.
        #                 current_batch_companies = []
        #                 if "meta_company_name" in verified_df.columns:
        #                     current_batch_companies = verified_df["meta_company_name"].astype(str).tolist()
        #                 elif "Company" in verified_df.columns:
        #                     current_batch_companies = verified_df["Company"].astype(str).tolist()
                        
        #                 # 3. Update Google Sheet
        #                 progress_text.text("🔄 Syncing verified data to Google Sheets...")
        #                 progress_bar.progress(30)
                        
        #                 updated_count = update_sheet_with_enriched_data(verified_df)
                        
        #                 if updated_count > 0:
        #                     st.success(f"✅ Updated {updated_count} rows in Google Sheets!")
        #                 else:
        #                     st.warning("⚠️ No rows updated (Check if Company Names match Google Sheet). Proceeding...")
                        
        #                 # 4. Generate Emails
        #                 progress_text.text("✍️ AI is writing personalized emails...")
        #                 progress_bar.progress(60)
                        
        #                 run_email_generation_layer()
                        
        #                 progress_bar.progress(90)
        #                 st.success("🎉 Process Complete! Emails have been drafted in the Google Sheet.")
                        
        #                 # ---------------------------------------------------------
        #                 # 👇 NEW LOGIC: FETCH ONLY CURRENT BATCH DATA 👇
        #                 # ---------------------------------------------------------
        #                 progress_text.text("📥 Fetching final data for download...")
                        
        #                 # Fetch ALL data from Sheet (this includes old and new data)
        #                 gc_final = connect_to_sheet()
        #                 sh_final = gc_final.open(GOOGLE_SHEET_NAME)
        #                 ws_final = sh_final.sheet1
        #                 all_data = ws_final.get_all_records()
                        
        #                 full_df = pd.DataFrame(all_data)

        #                 # Handle Column Name Variations (Normalize 'Company' to 'meta_company_name')
        #                 if "meta_company_name" not in full_df.columns and "Company" in full_df.columns:
        #                     full_df.rename(columns={"Company": "meta_company_name"}, inplace=True)

        #                 # --- CRITICAL FILTERING STEP ---
        #                 # Filter to keep ONLY rows that match the companies in our uploaded file
        #                 if current_batch_companies:
        #                     # Normalize names for better matching (strip spaces, lowercase)
        #                     batch_set = set(str(x).strip().lower() for x in current_batch_companies)
                            
        #                     # Apply the filter
        #                     final_df = full_df[full_df["meta_company_name"].astype(str).apply(lambda x: x.strip().lower()).isin(batch_set)]
        #                 else:
        #                     # Fallback: If no company names were found (rare), keep full data
        #                     final_df = full_df

        #                 # Keep only required columns for Instantly
        #                 target_cols = ["meta_company_name", "Email Subject", "Email Body", "CEO Name", "Email ID"]
        #                 available_cols = [c for c in target_cols if c in final_df.columns]
        #                 final_output = final_df[available_cols]

        #                 progress_bar.progress(100)
                        
        #                 # --- SAVE TO SESSION STATE TO PERSIST AFTER DOWNLOAD CLICK ---
        #                 # This ensures the download button remains visible even if the app reruns
        #                 st.session_state.final_csv_ready = final_output.to_csv(index=False).encode("utf-8")
                        
        #                 # Turn off the processing flag so we don't re-run logic unnecessarily
        #                 st.session_state.processing_emails = False 
                        
        #             except Exception as e:
        #                 st.error(f"❌ An error occurred: {e}")
        #                 st.session_state.processing_emails = False

        #     # ---------------------------------------------------------
        #     # SHOW DOWNLOAD BUTTON (Persistent)
        #     # ---------------------------------------------------------
        #     # This block is outside the button logic, so it stays visible
        #     if st.session_state.get("final_csv_ready"):
        #         st.divider()
        #         st.success("🚀 Ready for Instantly! Download only the NEW leads below.")
                
        #         st.download_button(
        #             label="📥 Download Final Leads (Only Current Batch)", 
        #             data=st.session_state.final_csv_ready, 
        #             file_name="Final_Instantly_Leads.csv", 
        #             mime="text/csv",
        #             key="final_download_btn_persistent"
        #         )

        #         st.info(" Note: Make sure your email has access to this sheet.")

        #         # Final Link Button
        #         st.link_button(
        #             label="📊 Open Google Sheet", 
        #             url="https://docs.google.com/spreadsheets/d/1yYKCYrILgvSrjeUObh2iE34tBie-Bkh6IIBAKiuaDRc/edit?gid=0#gid=0",
        #             type="primary"
        #         )

        # # --- ORIGINAL REPORT DOWNLOAD BUTTON (Outside the loop) ---
        # if st.session_state.df is not None:
        #     csv = st.session_state.df.to_csv(index=False).encode("utf-8")
        #     st.download_button(label=" Download Full Report (CSV)", data=csv, file_name=f"Report.csv", mime="text/csv")


        if st.session_state.show_leads:
            # qualified_companies = get_high_score_companies(company_df, threshold=25)#15
            qualified_companies = get_high_score_companies(company_df, threshold=qualification_threshold)

            st.markdown("### 🧠 Deep Company Intelligence")
            st.write(f"Qualified Companies: {len(qualified_companies)}")

            # --- MAIN PROCESS BUTTON ---
            if st.button("🚀 Generate Deep Company Reports"):
                # ---------------------------------------------------------
                # 👇 ADD THIS CLEANING BLOCK 👇
                # ---------------------------------------------------------
                import shutil
                
                # Folders to clean
                folders_to_clean = ["Unstructured_data", "structured_data"]
                
                for folder in folders_to_clean:
                    if os.path.exists(folder):
                        # Folder ke andar ki saari files delete karo
                        for filename in os.listdir(folder):
                            file_path = os.path.join(folder, filename)
                            try:
                                if os.path.isfile(file_path) or os.path.islink(file_path):
                                    os.unlink(file_path)
                                elif os.path.isdir(file_path):
                                    shutil.rmtree(file_path)
                            except Exception as e:
                                print(f"Failed to delete {file_path}. Reason: {e}")
                # ---------------------------------------------------------
                # 👆 END OF CLEANING 👆
                # ---------------------------------------------------------
                progress_text = st.empty()
                progress_bar = st.progress(0)

                # -------------------------------
                # STEP 1: Deep Research
                # -------------------------------
                progress_text.text("🔍 Running deep company research...")
                progress_bar.progress(10)

                asyncio.run(run_deep_research_for_companies(qualified_companies))

                progress_bar.progress(40)

                # -------------------------------
                # STEP 2: Cleaning & Structuring
                # -------------------------------
                progress_text.text("🧹 Cleaning & structuring company intelligence...")
                asyncio.run(clean_all_unstructured_reports_async(
                    unstructured_dir="Unstructured_data",
                    structured_dir="structured_data"
                ))

                progress_bar.progress(60)

                # Update JSONs with Streamlit scores
                update_structured_json_with_scores(company_df, job_q, loc_q)

                # -------------------------------
                # STEP 3: Upload to Sheets & AI Summary
                # -------------------------------
                progress_text.text("📤 Uploading structured data to Google Sheets...")
                
                struct_p = Path("structured_data")
                all_struct_files = list(struct_p.glob("*_Structured.json"))
                
                if all_struct_files:
                    upload_batch_data(all_struct_files)
                else:
                    st.error("⚠️ No files found in structured_data folder.")

                # Run AI Strategic Layer
                progress_bar.progress(70)
                progress_text.text("🤖 Generating AI Strategic Summaries...")
                run_ai_strategic_layer()

                # ---------------------------------------------------------
                # 👇 NEW LOGIC: ENRICHMENT & STOP 👇
                # ---------------------------------------------------------
                
                progress_bar.progress(85)
                progress_text.text("🧠 Extracting CEO & Contact Info via Groq...")
                
                # Run the enrichment script to create the CSV for verification
                csv_path = asyncio.run(run_data_enrichment())
                
                progress_bar.progress(100)

                if csv_path and "Failed" not in csv_path:
                    st.success("✅ Phase 1 Complete! Data available for verification below.")
                    
                    # 🔒 LOCK THE STATE: This triggers the Download Button to appear below
                    st.session_state.enrichment_ready = True
                    st.session_state.enrichment_csv = csv_path
                else:
                    st.error("❌ Data Enrichment Failed. Check logs.")


            # ---------------------------------------------------------
            # 👇 NEW SECTION: STEP 4 (Verify & Generate Emails) 👇
            # ---------------------------------------------------------
            st.divider()
            st.markdown("## 📧 Step 4: Verify & Generate Emails (Human-in-the-Loop)")

            # A. SHOW DOWNLOAD BUTTON (If Phase 1 is done)
            if st.session_state.get("enrichment_ready"):
                st.info("👇 **Action Required:** Download this file, add 'Email_ID' & 'CEO Name', then re-upload.")
                
                # Load CSV to show preview
                try:
                    if st.session_state.enrichment_csv and os.path.exists(st.session_state.enrichment_csv):
                        enrich_df = pd.read_csv(st.session_state.enrichment_csv)
                        st.dataframe(enrich_df.head(), use_container_width=True)
                        
                        with open(st.session_state.enrichment_csv, "rb") as f:
                            st.download_button(
                                label="📥 Download Enriched Data (CSV)",
                                data=f,
                                file_name="Enriched_Leads_For_Verification.csv",
                                mime="text/csv"
                            )
                    else:
                        st.warning("⚠️ CSV file not found. Please re-run Phase 1.")
                except Exception as e:
                    st.warning(f"Could not read generated CSV: {e}")

            # B. UPLOAD VERIFIED FILE & GENERATE EMAILS
            st.markdown("### 📤 Upload Verified Data to Start Emailing")
            verified_file = st.file_uploader("Upload the CSV with filled 'Email_ID' column", type=["csv"], key="email_uploader")

            if verified_file:
                # Trigger processing on button click
                if st.button("🚀 Sync Data & Generate Emails"):
                    st.session_state.processing_emails = True
                
                # Main Logic inside Session State check to prevent reset during interactions
                if st.session_state.get("processing_emails"):
                    progress_text = st.empty()
                    progress_bar = st.progress(0)
                    
                    try:
                        # 1. Read File
                    #     verified_file.seek(0)
                    #     verified_df = pd.read_csv(verified_file)
                        
                    #     rename_map = {
                    #     "Company_Name": "meta_company_name", 
                    #     "CEO_Full_Name": "CEO Name", 
                    #     "Email_ID": "Email ID"
                    # }
                    #     verified_df.rename(columns=rename_map, inplace=True)

                    # 1. Read File
                        verified_file.seek(0)
                        verified_df = pd.read_csv(verified_file)
                        
                        # ---------------------------------------------------
                        # 👇 DEMO SAVER FIX: CREATE BOTH COLUMN NAMES 👇
                        # ---------------------------------------------------
                        # Hum columns ko rename nahi karenge, duplicate banayenge.
                        # Taaki function ko jo naam pasand ho, wo mil jaye.
                        
                        # Fix Spaces
                        verified_df.columns = [c.strip() for c in verified_df.columns]

                        # 1. Company Name (Sheet needs 'meta_company_name')
                        if "Company_Name" in verified_df.columns:
                            verified_df["meta_company_name"] = verified_df["Company_Name"]
                            verified_df["Company"] = verified_df["Company_Name"]

                        # 2. Email (Sheet needs 'Email ID' or 'Email_ID')
                        if "Email_ID" in verified_df.columns:
                            verified_df["Email ID"] = verified_df["Email_ID"]
                            verified_df["Email"] = verified_df["Email_ID"]
                        
                        # 3. CEO (Sheet needs 'CEO Name')
                        if "CEO_Full_Name" in verified_df.columns:
                            verified_df["CEO Name"] = verified_df["CEO_Full_Name"]
                            verified_df["CEO"] = verified_df["CEO_Full_Name"]
                        # -----------------------------------------------------
                        # 🔍 SIMPLE & DIRECT COLUMN CHECK
                        # -----------------------------------------------------
                        # Since we generated the file, we expect 'meta_company_name' to be there.
                        # We will strictly use this list to filter the final sheet.
                        
                        target_company_col = "meta_company_name"
                        
                        # Clean columns (remove spaces just in case)
                        verified_df.columns = [c.strip() for c in verified_df.columns]
                        
                        if target_company_col not in verified_df.columns:
                            # Fallback: If user renamed it to "Company"
                            if "Company_Name" in verified_df.columns:  
                                verified_df.rename(columns={"Company_Name": target_company_col}, inplace=True)
                            elif "Company" in verified_df.columns:
                                verified_df.rename(columns={"Company": target_company_col}, inplace=True)
                            else:
                                st.error(f"❌ Error: The uploaded file is missing the '{target_company_col}' column.")
                                st.session_state.processing_emails = False
                                st.stop()
                        
                        # CAPTURE THE LIST OF COMPANIES YOU JUST UPLOADED
                        # This is the "Key" to getting the right data back.
                        current_batch_companies = verified_df[target_company_col].astype(str).str.strip().tolist()

                        # 3. Update Google Sheet
                        progress_text.text("🔄 Syncing verified data to Google Sheets...")
                        progress_bar.progress(30)
                        
                        updated_count = update_sheet_with_enriched_data(verified_df)
                        
                        if updated_count > 0:
                            st.success(f"✅ Updated {updated_count} rows in Google Sheets!")
                        else:
                            st.warning("⚠️ No rows updated. Duplicate Data")
                        
                        # 4. Generate Emails
                        progress_text.text("✍️ AI is writing personalized emails...")
                        progress_bar.progress(60)
                        
                        run_email_generation_layer()
                        
                        progress_bar.progress(90)
                        st.success("🎉 Emails generated in Google Sheet!")
                        
                        # ---------------------------------------------------------
                        # 👇 FINAL FILTER LOGIC: MATCHING UPLOADED COMPANIES 👇
                        # ---------------------------------------------------------
                        progress_text.text("📥 Preparing Final Download (Filtering for your uploaded companies)...")
                        
                        # Fetch ALL data from Sheet
                        gc_final = connect_to_sheet()
                        sh_final = gc_final.open(GOOGLE_SHEET_NAME)
                        ws_final = sh_final.sheet1
                        all_data = ws_final.get_all_records()
                        full_df = pd.DataFrame(all_data)

                        # Ensure 'meta_company_name' exists in the Sheet data
                        if "meta_company_name" not in full_df.columns and "Company" in full_df.columns:
                            full_df.rename(columns={"Company": "meta_company_name"}, inplace=True)

                        # FILTER: Keep only rows where 'meta_company_name' is in 'current_batch_companies'
                        # We normalize (lowercase/strip) to avoid case-sensitivity issues
                        
                        batch_set = set(str(x).lower().strip() for x in current_batch_companies)
                        
                        final_df = full_df[full_df["meta_company_name"].astype(str).str.lower().str.strip().isin(batch_set)]
                        
                        if final_df.empty:
                            st.error("❌ The filtered list is empty. Please check if Company Names in your CSV match the Google Sheet exactly.")
                        else:
                            # Keep only required columns for Instantly
                            target_cols = ["meta_company_name", "Email Subject", "Email Body", "CEO Name", "Email ID"]
                            available_cols = [c for c in target_cols if c in final_df.columns]
                            final_output = final_df[available_cols]

                            progress_bar.progress(100)
                            
                            # Save to Session State (Persistent)
                            st.session_state.final_csv_ready = final_output.to_csv(index=False).encode("utf-8")
                            st.session_state.processing_emails = False 
                        
                    except Exception as e:
                        st.error(f"❌ An error occurred: {e}")
                        st.session_state.processing_emails = False

            # ---------------------------------------------------------
            # SHOW DOWNLOAD BUTTON (Persistent)
            # ---------------------------------------------------------
            if st.session_state.get("final_csv_ready"):
                st.divider()
                st.success("🚀 Ready for Instantly! Download only the NEW leads below.")
                
                st.download_button(
                    label="📥 Download Final Leads (Only Current Batch)", 
                    data=st.session_state.final_csv_ready, 
                    file_name="Final_Instantly_Leads.csv", 
                    mime="text/csv",
                    key="final_download_btn_persistent"
                )

                st.info(" Note: Make sure your email has access to this sheet.")

                st.link_button(
                    label="📊 Open Google Sheet", 
                    url="https://docs.google.com/spreadsheets/d/1yYKCYrILgvSrjeUObh2iE34tBie-Bkh6IIBAKiuaDRc/edit?gid=0#gid=0",
                    type="primary"
                )

        # # --- ORIGINAL REPORT DOWNLOAD BUTTON (Outside the loop) ---
        # if st.session_state.df is not None:
        #     csv = st.session_state.df.to_csv(index=False).encode("utf-8")
        #     st.download_button(label=" Download Full Report (CSV)", data=csv, file_name=f"Report.csv", mime="text/csv")