
# import json
# import re
# import nltk
# import asyncio 
# import os
# from pathlib import Path
# from difflib import SequenceMatcher
# from nltk.corpus import stopwords
# from nltk.stem import WordNetLemmatizer

# # --- IMPORT THE UPLOADER MODULE ---
# from upload_to_sheets import upload_batch_data

# # -------------------------------------------------
# # NLTK SETUP (RUN ONCE)
# # -------------------------------------------------
# try:
#     nltk.data.find("tokenizers/punkt")
# except LookupError:
#     nltk.download("punkt")
#     nltk.download("stopwords")
#     nltk.download("wordnet")

# stop_words = set(stopwords.words("english"))
# lemmatizer = WordNetLemmatizer()

# # -------------------------------------------------
# # CONSTANTS & RULES
# # -------------------------------------------------

# BAD_DOMAINS = {
#     "linkedin.com", "facebook.com", "twitter.com", "x.com", "instagram.com",
#     "datanyze.com", "zoominfo.com", "crunchbase.com", "pitchbook.com",
#     "grammarly.com", "medium.com", "g2.com", "clutch.co", "glassdoor.com",
#     "goodfirms.co", "wikipedia.org", "youtube.com", "google.com", "gmail.com",
#     "yahoo.com", "outlook.com", "github.com", "upwork.com", "freelancer.com",
#     "tracxn.com"
# }

# LEGAL_WORDS = {
#     "technologies", "technology", "solutions", "software",
#     "systems", "services", "private", "limited", "pvt",
#     "ltd", "inc", "corp", "corporation", "llc",
#     "group", "consulting", "global", "analytics",
#     "ai", "data"
# }

# def safe_int(value):
#     try:
#         if value is None: return None
#         if isinstance(value, str) and value.strip() == "": return None
#         return int(value)
#     except: return None

# # -------------------------------------------------
# # UTILITY FUNCTIONS (SYNC - CPU BOUND)
# # -------------------------------------------------

# def strip_markdown_and_urls(text):
#     if not text: return ""
#     text = re.sub(r"!\[.*?\]\(.*?\)", " ", text)
#     text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)
#     text = re.sub(r"https?:\/\/\S+", " ", text)
#     return re.sub(r"\s+", " ", text).strip()

# def extract_brand_keyword(company_name: str) -> str:
#     clean = re.sub(r"[^a-zA-Z ]", "", company_name.lower())
#     words = clean.split()
#     core = [w for w in words if w not in LEGAL_WORDS]
#     return "".join(core if core else words)

# def find_closest_company_website(text, company_name):
#     if not text or not company_name: return None
#     brand = extract_brand_keyword(company_name)
#     url_pattern = r'\b(?:https?://|www\.)?([a-zA-Z0-9-]+\.(?:com|ai|io|co|net|org))\b'
#     candidates = []
#     for m in re.finditer(url_pattern, text.lower()):
#         domain = m.group(1)
#         if any(b in domain for b in BAD_DOMAINS): continue
#         stem = domain.split(".")[0]
#         score = SequenceMatcher(None, brand, stem).ratio()
#         candidates.append((score, domain))
#     if candidates:
#         candidates.sort(reverse=True)
#         if candidates[0][0] > 0.45: return f"https://{candidates[0][1]}"
#     return None

# def extract_competitors(raw_text, company_name=None):
#     competitors = set()
#     text = raw_text.replace("\n", " ")
#     tracxn_pattern = re.compile(r"Top competitors? of .*? include(.*?)(?:\.|\n|Here is)", re.I)
#     match = tracxn_pattern.search(text)
#     if match:
#         block = match.group(1)
#         links = re.findall(r"\[([A-Za-z0-9&.\- ]{2,50})\]\(", block)
#         competitors.update(links)
#         parts = re.split(r",| and ", block)
#         for p in parts:
#             p = strip_markdown_and_urls(p).strip()
#             if 2 <= len(p) <= 50: competitors.add(p)
    
#     cleaned = set()
#     for c in competitors:
#         c = re.sub(r"[^A-Za-z0-9&.\- ]", "", c).strip()
#         if not c or (company_name and c.lower() == company_name.lower()): continue
#         cleaned.add(c)
#     return sorted(cleaned)

# def extract_news(raw_text, company_name):
#     news = []
#     section_match = re.search(r"News related to .*?\n[-]+\n(.*?)(?:Get curated news|View complete company profile|$)", raw_text, re.S | re.I)
#     if not section_match: return news
#     news_block = section_match.group(1)
#     news_pattern = re.compile(r"\[([^\]]{10,200})\]\((https?:\/\/[^\)]+)\)(?:(?:\s+)?([A-Za-z ]+))?•([A-Za-z]{3} \d{2}, \d{4})•([^\n]+)", re.I)
#     for m in news_pattern.finditer(news_block):
#         related = re.findall(r"\[([A-Za-z0-9&.\- ]+)\]\(", m.group(5))
#         news.append({
#             "title": m.group(1).strip(), "url": m.group(2).strip(),
#             "source": m.group(3).strip() if m.group(3) else "Unknown",
#             "date": m.group(4).strip(),
#             "related_companies": list(set(related)) if related else [company_name]
#         })
#     return news

# def extract_leadership(raw_text):
#     leadership = {"founders": [], "board_members": [], "key_people": []}
#     ROLE_REGEX = re.compile(r"(co[- ]?founder|founder|chief executive officer|ceo|cto|cfo|vp|director|president)", re.I)
#     seen = set()
#     for line in raw_text.split("\n"):
#         clean = strip_markdown_and_urls(line)
#         if not ROLE_REGEX.search(clean): continue
#         name_match = re.search(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b", clean)
#         if not name_match: continue
#         name = name_match.group(1)
#         role_match = ROLE_REGEX.search(clean)
#         role_text = clean[role_match.start():].split(".")[0].strip()
#         if (name.lower(), role_text.lower()) in seen: continue
#         seen.add((name.lower(), role_text.lower()))
#         entry = {"name": name, "role": role_text}
#         if "founder" in role_text.lower(): leadership["founders"].append(entry)
#         elif any(x in role_text.lower() for x in ["board", "director"]): leadership["board_members"].append(entry)
#         else: leadership["key_people"].append(entry)
#     return leadership

# # -------------------------------------------------
# # DATA CLEANING & HELPERS
# # -------------------------------------------------

# def clean_text_light(text):
#     if not text: return ""
#     text = re.sub(r"\!\[.*?\]\(.*?\)", " ", text)
#     return re.sub(r"\s+", " ", text).strip()

# def extract_sentence_containing(text, keywords):
#     for s in re.split(r"[.\n]", text):
#         if any(k.lower() in s.lower() for k in keywords): return s.strip()
#     return None

# def extract_financials(text):
#     data = {}
 
#     text_l = text.lower()
 
#     revenue_patterns = [
#         r"\$([\d]+(?:\.\d+)?)\s*(b|b\+|bn|billion)\s*(?:annual\s*)?revenue",
#         r"\$([\d]+(?:\.\d+)?)\s*(m|m\+|million)\s*(?:annual\s*)?revenue",
#         r"annual revenue\s*\$([\d]+(?:\.\d+)?)\s*(b|bn|billion)",
#         r"annual revenue\s*\$([\d]+(?:\.\d+)?)\s*(m|million)"
#     ]
 
#     for pattern in revenue_patterns:
#         m = re.search(pattern, text_l, re.I)
#         if m:
#             value = float(m.group(1))
#             unit = m.group(2).lower()
 
#             if unit.startswith("b"):
#                 data["estimated_revenue_usd"] = f"${value}B"
#             else:
#                 data["estimated_revenue_usd"] = f"${value}M"
 
#             break  
 
#     if "estimated_revenue_usd" not in data:
#         matches = re.findall(r"\$([\d]+(?:\.\d+)?)\s*(b|m)", text_l, re.I)
 
#         best = None
#         for val, unit in matches:
#             val = float(val)
#             score = val * (1000 if unit.lower() == "b" else 1)
 
#             if not best or score > best[0]:
#                 best = (score, val, unit.upper())
 
#         if best:
#             data["estimated_revenue_usd"] = f"${best[1]}{best[2]}"
 
 
#     if m := re.search(r"employees?\s*[:\-]?\s*([\d,]+)", text, re.I):
#         emp = safe_int(m.group(1).replace(",", ""))
#         if emp:
#             data["employees"] = emp
 
#     return data

# def extract_locations(text):
#     common = ["India", "USA", "United States", "UK", "UAE", "Canada", "Australia"]
#     return sorted({c for c in common if c.lower() in text.lower()})

# def extract_emails(text):
#     return sorted(set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)))

# def extract_phone_numbers(text):
#     matches = re.findall(r"(?:\+?\d{1,3}[\s\-]?)?(?:\(?\d{2,4}\)?[\s\-]?)?\d{3,4}[\s\-]?\d{3,4}", text)
#     return sorted({m for m in matches if 8 <= len(re.sub(r"\D", "", m)) <= 15})

# # -------------------------------------------------
# #  MAIN EXTRACTION LOGIC (SYNC)
# # -------------------------------------------------

# def extract_company_intelligence(input_json, output_json):
#     """
#     Core function to clean raw data and save structured JSON.
#     """
#     input_json = Path(input_json)
#     output_json = Path(output_json)

#     if not input_json.exists():
#         raise FileNotFoundError(f"Input JSON not found: {input_json}")

#     with input_json.open("r", encoding="utf-8") as f:
#         data = json.load(f)

#     company_name = data.get("meta", {}).get("company_name", "")

#     combined_raw_text = ""
#     for r in data.get("financial_intelligence", []):
#         combined_raw_text += "\n" + r.get("content", "")
#         combined_raw_text += "\n" + r.get("raw_content", "")

#     clean_light = clean_text_light(combined_raw_text)

#     output = {
#         "meta": data.get("meta", {}),
#         "company_profile": {
#             "company_name": company_name,
#             "website": find_closest_company_website(combined_raw_text, company_name),
#             "industry": extract_sentence_containing(clean_light, ["industry"]),
#             "tagline": extract_sentence_containing(clean_light, ["specializing", "leader", "delivering"]),
#         },
#         "leadership_team": extract_leadership(combined_raw_text),
#         "competitors": extract_competitors(combined_raw_text, company_name),
#         "news": extract_news(combined_raw_text, company_name),
#         "financials": extract_financials(clean_light),
#         "locations": extract_locations(clean_light),
#         "contact_information": {
#             "emails": extract_emails(clean_light),
#             "phone_numbers": extract_phone_numbers(clean_light)
#         }
#     }

#     # Ensure output directory exists
#     output_json.parent.mkdir(parents=True, exist_ok=True)

#     with output_json.open("w", encoding="utf-8") as f:
#         json.dump(output, f, indent=4, ensure_ascii=False)

#     return f" Cleaned: {company_name}"


# # -------------------------------------------------
# #  ASYNC PIPELINE: CLEANING + BATCH UPLOAD
# # -------------------------------------------------

# async def clean_all_unstructured_reports_async(
#     unstructured_dir="Unstructured_data",
#     structured_dir="structured_data"
# ):
#     """
#     Manages the cleaning pipeline in batches to optimize for API limits.
#     Logic: Clean 10 files -> Upload 10 files -> Wait 10 seconds.
#     """
#     unstructured_dir = Path(unstructured_dir)
#     structured_dir = Path(structured_dir)

#     if not unstructured_dir.exists():
#         print(f" Unstructured directory not found: {unstructured_dir}")
#         return

#     structured_dir.mkdir(parents=True, exist_ok=True)
#     all_files = list(unstructured_dir.glob("*_Report.json"))
    
#     total_files = len(all_files)
#     print(f" Total Files to Process: {total_files}")
    
#     # --- CONFIGURATION ---
#     BATCH_SIZE = 10   # Number of files to process per batch
#     DELAY = 10        # Seconds to wait after uploading a batch
#     # ---------------------

#     # Iterate through files in chunks of 10
#     for i in range(0, total_files, BATCH_SIZE):
        
#         # 1. Identify the current batch
#         current_batch_files = all_files[i : i + BATCH_SIZE]
#         batch_num = (i // BATCH_SIZE) + 1
        
#         print(f"\n Processing Batch {batch_num} (Files {i+1} to {min(i+BATCH_SIZE, total_files)})...")
        
#         # 2. Define a task wrapper to clean files and return the path
#         async def process_and_return_path(file):
#             output_path = structured_dir / file.name.replace("_Report.json", "_Structured.json")
#             try:
#                 # Run CPU-bound cleaning in a thread
#                 await asyncio.to_thread(extract_company_intelligence, file, output_path)
#                 return output_path
#             except Exception as e:
#                 print(f" Error cleaning {file.name}: {e}")
#                 return None

#         # 3. Clean the batch in parallel
#         tasks = [process_and_return_path(f) for f in current_batch_files]
#         results = await asyncio.gather(*tasks)
        
#         # Filter out failed files (None values)
#         processed_file_paths = [res for res in results if res is not None]
        
#         # 4. Upload the structured files to Google Sheets
#         if processed_file_paths:
#             print(f" Uploading batch of {len(processed_file_paths)} files to Google Sheets...")
#             await asyncio.to_thread(upload_batch_data, processed_file_paths)
        
#         # 5. API Safety Delay
#         if i + BATCH_SIZE < total_files:
#             print(f" Cooling down for {DELAY} seconds to prevent API limits...")
#             await asyncio.sleep(DELAY)

#     print("\n All Batches Completed Successfully.")

# # --- ENTRY POINT ---
# if __name__ == "__main__":
#     asyncio.run(clean_all_unstructured_reports_async())






import json
import re
import asyncio 
import os
from pathlib import Path
from difflib import SequenceMatcher

# --- IMPORT THE UPLOADER MODULE ---
from upload_to_sheets import upload_batch_data

# -------------------------------------------------
# CONSTANTS FOR WEBSITE FINDING
# -------------------------------------------------

BAD_DOMAINS = {
    "linkedin.com", "facebook.com", "twitter.com", "x.com", "instagram.com",
    "datanyze.com", "zoominfo.com", "crunchbase.com", "pitchbook.com",
    "grammarly.com", "medium.com", "g2.com", "clutch.co", "glassdoor.com",
    "goodfirms.co", "wikipedia.org", "youtube.com", "google.com", "gmail.com",
    "yahoo.com", "outlook.com", "github.com", "upwork.com", "freelancer.com",
    "tracxn.com", "rocketreach.co", "zaubacorp.com", "tofler.in", "thecompanycheck.com"
}

LEGAL_WORDS = {
    "technologies", "technology", "solutions", "software",
    "systems", "services", "private", "limited", "pvt",
    "ltd", "inc", "corp", "corporation", "llc",
    "group", "consulting", "global", "analytics",
    "ai", "data"
}

# -------------------------------------------------
# WEBSITE EXTRACTION LOGIC (Keep this as requested)
# -------------------------------------------------

def extract_brand_keyword(company_name: str) -> str:
    clean = re.sub(r"[^a-zA-Z ]", "", company_name.lower())
    words = clean.split()
    core = [w for w in words if w not in LEGAL_WORDS]
    return "".join(core if core else words)

def find_closest_company_website(text, company_name):
    """
    Scans the raw text to find the most likely company website.
    """
    if not text or not company_name: return None
    brand = extract_brand_keyword(company_name)
    
    # Regex to find domain-like patterns
    url_pattern = r'\b(?:https?://|www\.)?([a-zA-Z0-9-]+\.(?:com|ai|io|co|net|org|in))\b'
    
    candidates = []
    for m in re.finditer(url_pattern, text.lower()):
        domain = m.group(1)
        
        # Skip known bad domains
        if any(b in domain for b in BAD_DOMAINS): continue
        
        stem = domain.split(".")[0]
        score = SequenceMatcher(None, brand, stem).ratio()
        candidates.append((score, domain))
    
    if candidates:
        candidates.sort(reverse=True)
        # Threshold: 0.45 match score
        if candidates[0][0] > 0.45: return f"https://{candidates[0][1]}"
    
    return None

# -------------------------------------------------
#  MAIN LOGIC
# -------------------------------------------------

def extract_company_intelligence(input_json, output_json):
    """
    Simplifies the JSON: Extracts Website URL and passes everything else RAW.
    """
    input_json = Path(input_json)
    output_json = Path(output_json)

    if not input_json.exists():
        raise FileNotFoundError(f"Input JSON not found: {input_json}")

    with input_json.open("r", encoding="utf-8") as f:
        data = json.load(f)

    company_name = data.get("meta", {}).get("company_name", "")

    # Combine text ONLY for finding the website
    combined_raw_text = ""
    for r in data.get("financial_intelligence", []):
        combined_raw_text += " " + r.get("url", "") + " " + r.get("content", "")

    # 1. Find Website
    website_url = find_closest_company_website(combined_raw_text, company_name)

    # 2. Structure the Output (As per your requirement)
    output = {
        "meta": data.get("meta", {}),
        
        "company_profile": {
            "company_name": company_name,
            "website": website_url  # <--- Calculated URL
            
        },
        
        # Pass RAW Data (No Cleaning)
        "financial_intelligence": data.get("financial_intelligence", []),
        "market_updates": data.get("market_updates", []),

        "search_context": {
            "target_job_role": "", 
            "target_location": ""
        },
        
        # Add Lead Scoring placeholders (Taaki Google Sheet me column ban jaye)
        "lead_scoring": {
            "lead_score": "",        # Will be filled by lead_scoring.py later
            "rank_breakout": ""      # Will be filled by lead_scoring.py later
        },

        "found_job_titles": "",
        "open_roles_count": "",

          
        "AI Strategic Summary": ""   # Placeholder
    }

    # Ensure output directory exists
    output_json.parent.mkdir(parents=True, exist_ok=True)

    with output_json.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=4, ensure_ascii=False)

    return f" Processed: {company_name}"


# -------------------------------------------------
#  ASYNC PIPELINE: BATCH UPLOAD
# -------------------------------------------------

async def clean_all_unstructured_reports_async(
    unstructured_dir="Unstructured_data",
    structured_dir="structured_data"
):
    unstructured_dir = Path(unstructured_dir)
    structured_dir = Path(structured_dir)

    if not unstructured_dir.exists():
        print(f" Unstructured directory not found: {unstructured_dir}")
        return

    structured_dir.mkdir(parents=True, exist_ok=True)
    all_files = list(unstructured_dir.glob("*_Report.json"))
    
    total_files = len(all_files)
    print(f" Total Files to Process: {total_files}")
    
    BATCH_SIZE = 10   
    DELAY = 5        

    for i in range(0, total_files, BATCH_SIZE):
        
        current_batch_files = all_files[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        
        print(f"\n Processing Batch {batch_num} (Files {i+1} to {min(i+BATCH_SIZE, total_files)})...")
        
        async def process_and_return_path(file):
            output_path = structured_dir / file.name.replace("_Report.json", "_Structured.json")
            try:
                await asyncio.to_thread(extract_company_intelligence, file, output_path)
                return output_path
            except Exception as e:
                print(f" Error processing {file.name}: {e}")
                return None

        tasks = [process_and_return_path(f) for f in current_batch_files]
        results = await asyncio.gather(*tasks)
        
        processed_file_paths = [res for res in results if res is not None]
        
        if processed_file_paths:
            print(f" Uploading batch of {len(processed_file_paths)} files to Google Sheets...")
            await asyncio.to_thread(upload_batch_data, processed_file_paths)
        
        if i + BATCH_SIZE < total_files:
            print(f" Cooling down for {DELAY} seconds...")
            await asyncio.sleep(DELAY)

    print("\n All Batches Completed Successfully.")

if __name__ == "__main__":
    asyncio.run(clean_all_unstructured_reports_async())