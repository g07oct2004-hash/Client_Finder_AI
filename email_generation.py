import os
import json
import asyncio
import pandas as pd
from dotenv import load_dotenv
from groq import AsyncGroq
import gspread
from google.oauth2.service_account import Credentials
from API_rotation import get_groq_key, get_groq_count

# --- 1. SETUP ---
load_dotenv()

try:
    from upload_to_sheets import GOOGLE_SHEET_NAME
except ImportError:
    GOOGLE_SHEET_NAME = "Email_tool" 

# --- 2. GOOGLE SHEETS CONNECTION ---
def connect_to_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"Connection Failed: {e}")
        return None

# --- 3. AI GENERATION LOGIC ---
async def generate_email_content(data, sem):
    """
    Generates Subject and Body using the specific columns provided.
    """
    company = data.get("company", "the company")
    role = data.get("role", "tech roles")
    ceo_name = data.get("ceo_name", "there")
    financials = data.get("financials", "")[:2000] # Limit text length
    market_news = data.get("market", "")[:2000]
    score = data.get("score", "0")
    breakout = data.get("breakout", "")
    open_roles = data.get("open_roles", 1)

#     prompt = f"""
# Act as a B2B Sales Expert. Write a personalized cold email to **{company}**.

# **CONTEXT:**
# - **Target Role:** They are hiring for: {role}
# - **Financial Intel:** {financials}
# - **Market News:** {market_news}
# - **Lead Strength:** {score}/100 ({breakout})

# **YOUR GOAL:**
# Write an email pitching our "AI-Powered Salesforce & Automation Services".
# 1. **Hook:** Reference the open role ({role}) or recent news.
# 2. **Value:** Explain how we can help them scale this specific function without increasing headcount.
# 3. **CTA:** Ask for a 15-min chat.

# **STRICT OUTPUT FORMAT:**
# You must output the response in this exact format with the separators:

# SUBJECT: [Insert Subject Here]
# BODY_START
# [Insert Email Body Here]
# BODY_END
# """

#     prompt = f"""

# You are a senior B2B sales copywriter who specializes in highly personalized outbound emails for technology and consulting companies.

# **DATA CONTEXT:**
# - **Company Name:** {company}
# - **Target Role (They are hiring):** {role}
# - **Financial Intel:** {financials}
# - **Market News:** {market_news}
# - **Open Roles Count:** {open_roles}
# - **Hiring Volume Signal (Internal Data):** Score_breakout {breakout} (Logic: 5 score is for 1 Job role that means IF job openings has an 10 then 2 job roles or if 15 then 3 jOB Roles).

# **YOUR POSITIONING:**
# "We help growing tech companies scale faster by combining Salesforce CRM, AI, and data intelligence into a single smart delivery engine—without increasing headcount."

# **GUIDELINES:**
# - **Read carefully:** Use the provided intelligence to reference their growth stage, focus areas, or recent activity.
# - **Interpret Hiring Volume:** NEVER mention the "Lead Score" number or the word "breakout" in the email. Instead, translate the score into context:
#     - If it contains **"+5 (Job Volume)"**: Mention they are looking for a skilled {role}.
#     - If it contains **"+10 (Job Volume)"**: Mention they are "expanding their team" or looking for "multiple {role}s".
#     - - If it contains **"+15 (Job Volume)"** or higher: Mention they are "scaling their engineering team" with several {role} openings.
# - **Soft Personalization:** Do NOT invent facts. Use the data to make genuine observations.
# - **Pain Points:** Analyze the dataset to infer likely pain points (e.g., delivery bandwidth, scaling gaps, AI adoption, CRM maturity).
# - **Solution:** Position us as a strategic execution partner in **Salesforce + AI development**. Explain *how* we help (speed/efficiency), not just what we do.
# - **Tone:** Keep it concise, confident, human, and respectful.
# - **Structure:**
#     1. Short contextual opener showing genuine awareness (hook based on the Role or News).
#     2. Clear observation regarding their hiring volume (derived from the score logic above).
#     3. Value proposition in 2–3 bullets tailored to their likely pain points.
#     4. Short, low-friction CTA (10–15 minute chat).

# **STRICT OUTPUT FORMAT:**
# You must output the response in this exact format with the separators (The code depends on this):

# SUBJECT: [Relevant and slightly urgent subject line, not salesy]
# BODY_START
# [Insert Email Body Here]
# BODY_END
# """

    prompt = f"""
You are a senior B2B sales copywriter who specializes in highly personalized outbound emails for technology and consulting companies.

**DATA CONTEXT:**
- **Company Name:** {company}
- **Target Role (They are hiring):** {role}
- **Target Contact (Name):** {ceo_name}
- **Financial Intel:** {financials}
- **Market News:** {market_news}
- **Open Roles Count:** {open_roles}

**YOUR POSITIONING:**
"We help growing tech companies scale faster by combining Salesforce CRM, AI, and data intelligence into a single smart delivery engine—without increasing headcount."

**GUIDELINES:**
- **Addressing:** Start the email strictly with "Hi {ceo_name},"
- **Read carefully:** Use the provided intelligence to reference their growth stage, focus areas, or recent activity.
- **Interpret Hiring Volume:** Use the provided 'Open Roles Count' to frame their growth context:
    - If **1 role**: Mention they are looking for a key {role}.
    - If **2 roles**: Mention they are "expanding their team" with multiple {role}s.
    - If **3 or more roles**: Mention they are "scaling their engineering team" with several {role} openings.
- **Soft Personalization:** Do NOT invent facts. Use the data to make genuine observations.
- **Pain Points:** Analyze the dataset to infer likely pain points (e.g., delivery bandwidth, scaling gaps, AI adoption, CRM maturity).
- **Solution:** Position us as a strategic execution partner in **Salesforce + AI development**. Explain *how* we help (speed/efficiency), not just what we do.
- **Tone:** Keep it concise, confident, human, and respectful.
- **Structure:**
    1. Short contextual opener showing genuine awareness (hook based on the Role or News).
    2. Clear observation regarding their hiring volume (derived from the Open Roles logic above).
    3. Value proposition in 2–3 bullets tailored to their likely pain points.
    4. Short, low-friction CTA (10–15 minute chat).

**STRICT OUTPUT FORMAT:**
You must output the response in this exact format with the separators (The code depends on this):

SUBJECT: [Relevant and slightly urgent subject line, not salesy]
BODY_START
[Insert Email Body Here]
BODY_END
"""
    async with sem:
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
                print(f" Groq Attempt {attempt+1} Failed: {e}")
                await asyncio.sleep(1)
        
        return None

# --- 4. ROW PROCESSING ---
async def process_row(index, row, worksheet, subject_col_idx, body_col_idx, sem):
    # 1. Check if Email already exists (Safety Check)
    existing_subject = str(row.get("Email Subject", "")).strip()
    existing_body = str(row.get("Email Body", "")).strip()
    
    if len(existing_subject) > 5 or len(existing_body) > 10:
        return 0  # Skip, already done

    # 2. Extract Data from specific columns
    company_name = row.get("meta_company_name") or row.get("Company")
    if not company_name: return 0

    print(f" Generating for: {company_name}")
    

    try:
        role_count = int(row.get("open_roles_count", 1))
    except:
        role_count = 1
    input_data = {
        "company": company_name,
        "ceo_name": row.get("CEO Name", "there"),
        "score": row.get("lead_scoring_lead_score", "0"),
        "breakout": row.get("lead_scoring_rank_breakout", ""),
        "role": row.get("search_context_target_job_role", "Salesforce Specialist"),
        "financials": row.get("financial_intelligence", ""),
        "market": row.get("market_updates", ""),
        "open_roles": role_count
    }

    # 3. Generate Content
    raw_response = await generate_email_content(input_data, sem)
    
    if not raw_response:
        print(f" Failed to generate for {company_name}")
        return 0

    # 4. Parse Response (Split Subject and Body)
    try:
        subject = ""
        body = ""
        
        lines = raw_response.split('\n')
        parsing_body = False
        body_lines = []

        for line in lines:
            if line.startswith("SUBJECT:"):
                subject = line.replace("SUBJECT:", "").strip()
            elif "BODY_START" in line:
                parsing_body = True
            elif "BODY_END" in line:
                parsing_body = False
            elif parsing_body:
                body_lines.append(line)
        
        body = "\n".join(body_lines).strip()

        # Fallback if parsing failed
        if not subject: subject = f"Question about {input_data['role']} at {company_name}"
        if not body: body = raw_response

        # 5. Update Sheet (Same Row)
        # Note: gspread uses 1-based indexing. Dataframe index is 0-based.
        # Row number = index + 2 (1 for header + 1 for 0-index offset)
        row_num = index + 2
        
        worksheet.update_cell(row_num, subject_col_idx, subject)
        worksheet.update_cell(row_num, body_col_idx, body)
        
        print(f" Updated Sheet for {company_name}")
        return 1

    except Exception as e:
        print(f"Error updating sheet for {company_name}: {e}")
        return 0

# --- 5. MAIN EXECUTION ---
async def main_async():
    gc = connect_to_sheet()
    if not gc: return

    try:
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.sheet1
    except Exception as e:
        print(f"Could not open sheet: {e}")
        return

    # Get all data to find headers
    raw_data = worksheet.get_all_values()
    if not raw_data: return

    headers = raw_data[0]
    # create DataFrame with matching headers
    df = pd.DataFrame(raw_data[1:], columns=headers)

    # Helper to find or create column index
    def get_or_create_col(col_name):
        if col_name in headers:
            return headers.index(col_name) + 1
        else:
            print(f"Creating new column: {col_name}")
            worksheet.add_cols(1)
            new_idx = len(headers) + 1
            worksheet.update_cell(1, new_idx, col_name)
            headers.append(col_name) # Update local headers list
            return new_idx

    # Get Column Indices for Subject and Body
    subject_col_idx = get_or_create_col("Email Subject")
    body_col_idx = get_or_create_col("Email Body")

    sem = asyncio.Semaphore(3) # Process 3 emails at a time
    tasks = []

    print(" Starting Email Generation Pipeline...")

    for index, row in df.iterrows():
        # Only process if we have a Company Name
        if row.get("meta_company_name") or row.get("Company"):
            task = process_row(index, row, worksheet, subject_col_idx, body_col_idx, sem)
            tasks.append(task)

    results = await asyncio.gather(*tasks)
    print(f" Finished! Generated {sum(results)} new emails.")

def run_email_generation_layer():
    asyncio.run(main_async())

if __name__ == "__main__":
    run_email_generation_layer()