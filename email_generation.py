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
#     prompt = f"""
# You are a senior B2B sales copywriter who specializes in highly personalized outbound emails for technology and consulting companies.
 
# **DATA CONTEXT:**
# - **Company Name:** {company}
# - **Target Role (They are hiring):** {role}
# - **Target Contact (Name):** {ceo_name}
# - **Financial Intel:** {financials}
# - **Market News:** {market_news}
# - **Open Roles Count:** {open_roles}
 
# **YOUR POSITIONING:**
# "We help growing tech companies scale faster by combining Salesforce CRM, AI, and data intelligence into a single smart delivery engine—without increasing headcount."
 
# **GUIDELINES:**
# - **Addressing:** Start the email strictly with "Hi {ceo_name},"
# - **Read carefully:** Use the provided intelligence to reference their growth stage, focus areas, or recent activity.
# - **Interpret Hiring Volume:** Use the provided 'Open Roles Count' to frame their growth context:
#     - If **1 role**: Mention they are looking for a key {role}.
#     - If **2 roles**: Mention they are "expanding their team" with multiple {role}s.
#     - If **3 or more roles**: Mention they are "scaling their engineering team" with several {role} openings.
# - **Soft Personalization:** Do NOT invent facts. Use the data to make genuine observations.
# - **Pain Points:** Analyze the dataset to infer likely pain points (e.g., delivery bandwidth, scaling gaps, AI adoption, CRM maturity).
# - **Solution:** Position us as a strategic execution partner in **Salesforce + AI development**. Explain *how* we help (speed/efficiency), not just what we do.
# - **Tone:** Keep it concise, confident, human, and respectful.
# - **Structure:**
#     1. Short contextual opener showing genuine awareness (hook based on the Role or News).
#     2. Clear observation regarding their hiring volume (derived from the Open Roles logic above).
#     3. Value proposition in 2–3 bullets tailored to their likely pain points.
#     4. Short, low-friction CTA (10–15 minute chat).
#     5. DO NOT include signature or [Your Name] in the End.
 
# **STRICT OUTPUT FORMAT:**
# You must output the response in this exact format with the separators (The code depends on this):
 
# SUBJECT: [Relevant and slightly urgent subject line, not salesy]
# BODY_START
# [Insert Email Body Here]
# BODY_END
# """
#------
#-----------------------------------
#-----------------------------------
#     prompt = f"""
# You are a senior B2B sales copywriter who specializes in writing highly personalized, one-to-one outbound emails for technology and consulting companies.
# Your goal is to write emails that feel genuinely human, thoughtful, natural, and manually written — never templated, robotic, or marketing-heavy.

# **DATA CONTEXT:**
# - **Company Name:** {company}
# - **Target Role (They are hiring):** {role}
# - **Target Contact (Name):** {ceo_name}
# - **Financial Intel:** {financials}
# - **Market News:** {market_news}
# - **Open Roles Count:** {open_roles}

# **OUR SERVICE DETAILS (Use strictly based on role relevance):**

# **If AI / Data / Automation-focused — **
# We design and deploy production-grade AI and data analytics solutions that automate business processes, generate predictive insights, and improve decision-making. Our expertise includes custom AI models, generative AI, chatbots, NLP, predictive analytics, and intelligent automation, helping businesses reduce costs and scale efficiently.

# **If Salesforce-focused — **
# We provide end-to-end Salesforce consulting, implementation, customization, and integration services to optimize sales, service, and CRM operations. Our solutions help organizations improve pipeline visibility, automate workflows, enhance customer engagement, and maximize ROI from Salesforce.

# **If both Salesforce + AI appear:**
# Naturally blend both descriptions into a single concise value proposition without repetition.

# **HUMAN THINKING MODE (CRITICAL):**
# Before writing the email, think like a real human sales professional:
# - Why would *you personally* reach out to this person?
# - What practical challenges are companies in this situation usually facing?
# - What simple, natural observation could start a real conversation?

# Write the email as if you are sending a personal note, not running a campaign.
# Slight unevenness in phrasing and rhythm is natural and encouraged


# **HUMANIZATION & SPAM-FREE RULES:**
# - Write in a natural, conversational human tone — never marketing or robotic.
# - Avoid buzzwords, hype, urgency triggers, and promotional language.
# - Prefer short, simple, direct sentences.
# - The email must feel like a genuine 1-to-1 note, not a bulk message.
# - No: “Hope you’re doing well”, “I hope this email finds you well”, or formal openers.
# - Use natural contractions (don’t, we’re, it’s).
# - Sound like a busy professional writing quickly and thoughtfully.
# - Do NOT sound like a company brochure or website.
# - If a sentence feels like marketing copy, rewrite it in simple spoken English.


# - **ZERO SIGNATURE:** Stop the email immediately after the Call to Action. Do not add "Best," "Regards," or "[Your Name]."

# **SERVICE FOCUS LOGIC:**
# - If the Target Role or Open Roles mention Salesforce, CRM, RevOps, Sales Ops, or Salesforce Developer → focus on Salesforce services.
# - If the Target Role or Open Roles mention AI, ML, Data, Automation, Engineering → focus on AI & automation services.
# - If both appear → blend Salesforce + AI naturally.

# **GUIDELINES:**

# - **Addressing (CRITICAL):**
#   Always greet using ONLY the first name of the contact.
#   If {ceo_name} contains a full name, extract and use only the first word.
#   Examples:
#   - "Kumar Sharma" → "Hi Kumar,"
#   - "John Michael Doe" → "Hi John,"
#   Never use last name, full name, Mr., Ms., or any title.
#   Format must always be: "Hi <FirstName>,"
# - **Read carefully:** Use the provided intelligence to reference their growth stage, focus areas, or recent activity.
# - **Interpret Hiring Volume:** Use the provided 'Open Roles Count' to frame their growth context:
#     - If **1 role**: Mention they are looking for a key {role}.
#     - If **2 roles**: Mention they are "expanding their team" with multiple {role}s.
#     - If **3 or more roles**: Mention they are "scaling their engineering team" with several {role} openings.
# - **Soft Personalization:** Do NOT invent facts. Use the data to make genuine observations.
# - **Pain Points:** Analyze the dataset to infer likely pain points (e.g., delivery bandwidth, scaling gaps, AI adoption, CRM maturity).
# - **Solution:** Position us as a strategic execution partner in **Salesforce + AI development**. Explain *how* we help (speed/efficiency), not just what we do.
# - **Tone:** Keep it concise, confident, human, and respectful.
# - **Structure:**
#     1. Short contextual opener showing genuine awareness (hook based on the Role or News).
#     2. Clear observation regarding their hiring volume (derived from the Open Roles logic above).
#     3. Value proposition in 2–3 bullets tailored to their likely pain points.
#     4. **Strictly Follow**:- Short, low-friction CTA (10–15 minute chat), strictly aligned with service focus:
#         - If Salesforce-focused → mention only Salesforce support/services.
#         - If AI-focused → mention only AI / automation services.
#         - If both → mention Salesforce + AI together.
#         - The CTA must sound like a real person asking for time, not a sales pitch.
#             Example tone:
#             "Worth a quick 10–15 min chat to compare notes?"
#     5. DO NOT include signature or [Your Name] in the End.

# **ANTI-AI DETECTION RULE (CRITICAL):**
# Avoid:
# - Perfect symmetry
# - Over-polished structure
# - Corporate phrasing
# - Predictable patterns
# Allow:
# - Natural rhythm
# - Slight phrasing variation
# - Human sentence flow


# **STRICT OUTPUT FORMAT:**
# You must output the response in this exact format with the separators (The code depends on this):

# SUBJECT: [Relevant and slightly urgent subject line, not salesy]
# BODY_START
# [Insert Email Body Here]
# BODY_END
# """ 
# -------------------------------
# --------------------------------
# ------
#     prompt = f"""
# You are a senior B2B sales copywriter who specializes in writing natural, highly personalized, one-to-one outbound emails for technology and consulting companies. Your writing must feel human, thoughtful, and genuinely researched — never templated or promotional.

# **DATA CONTEXT:**
# - **Company Name:** {company}
# - **Target Role (They are hiring):** {role}
# - **Target Contact (Name):** {ceo_name}
# - **Financial Intel:** {financials}
# - **Market News:** {market_news}
# - **Open Roles Count:** {open_roles}

# **YOUR POSITIONING (INTERNAL UNDERSTANDING ONLY):**
# "We help growing technology companies deliver Salesforce and AI initiatives faster and with lower execution risk by acting as an extended engineering and intelligence arm — without increasing headcount."

# **SERVICE INTELLIGENCE (INTERNAL REFERENCE — NEVER LIST SERVICES):**

# We operate across three core problem areas:

# 1) **Salesforce Execution & CRM Scaling**
#    - Used when Salesforce delivery bandwidth is tight
#    - Custom workflows, integrations, and automation needs grow
#    - Internal teams struggle to keep pace with CRM roadmap

# 2) **AI & Intelligent Automation**
#    - Used when teams want predictive insights, automation, or AI-driven workflows
#    - Useful for sales forecasting, lead scoring, intelligent agents, and operational automation

# 3) **Unified Smart CRM (Salesforce + AI + Data)**
#    - Used for high-growth teams scaling rapidly
#    - Focused on automation, insight generation, and reducing dependency on large internal engineering teams

# **CRITICAL RULE:**
# Translate services into **business outcomes and operational benefits**. Never list services or sound like a brochure.

# ---

# **HUMANIZATION MODE (VERY IMPORTANT):**
# Write as a thoughtful business professional sending a genuine 1-to-1 email.
# - Sound curious, observant, and respectful.
# - Avoid marketing tone.
# - Avoid corporate buzzwords.
# - Use natural sentence flow.
# - Vary sentence length.
# - Light conversational style.
# - No exaggerated claims.

# **LANGUAGE STYLE RULES:**
# - Simple business English.
# - Avoid: leverage, synergy, optimize, streamline, unlock, transformation.
# - Prefer:
#   - "I noticed..." instead of "We observed"
#   - "It looks like..." instead of "Based on analysis"
#   - "Thought this might be relevant..." instead of "We help companies"
# - Use contractions naturally (we’re, it’s, you’re).

# ---

# **PERSONALIZATION LOGIC:**

# - If **open_roles == 1**:
#   Mention they’re hiring for a key {role}.

# - If **open_roles == 2**:
#   Mention they’re expanding their team.

# - If **open_roles >= 3**:
#   Mention they’re scaling their engineering or delivery team.

# Use this only as **contextual framing**, not as direct selling.

# ---

# **EMAIL STRUCTURE:**

# 1) Short natural opener referencing hiring or recent company activity.
# 2) Gentle observation about growth, delivery pressure, or scaling.
# 3) A short conversational paragraph explaining how we typically help companies at this stage — using outcomes, not services.
# 4) Soft, pressure-free close (no sales CTA, no meeting selling language).
# 5) DO NOT include signature or [Your Name] in the End.
# ---

# **STRICT OUTPUT FORMAT (MANDATORY):**

# SUBJECT: [Natural, human subject line — no marketing or sales tone]
# BODY_START
# [Email body]
# BODY_END
# """



#     prompt = f"""
# You are a senior B2B sales copywriter who specializes in writing highly personalized, one-to-one outbound emails for technology and consulting companies.
# Your goal is to write emails that feel hyper Personalize ,genuinely human, thoughtful, natural, and manually written — never templated, robotic, or marketing-heavy.

# **DATA CONTEXT:**
# - **Company Name:** {company}
# - **Target Role (They are hiring):** {role}
# - **Target Contact (Name):** {ceo_name}
# - **Financial Intel:** {financials}
# - **Market News:** {market_news}
# - **Open Roles Count:** {open_roles}

# **OUR SERVICE DETAILS (Use strictly based on role relevance):**

# **If AI / Data / Automation-focused — **
# Highlight the availability of certified AI, data science, ML, and automation experts who are already screened, tested, and ready-to-work. Emphasize flexible hiring models such as contract, contract-to-hire, or remote staffing that fit the recipient’s timelines and budget. Showcase speed by noting that qualified AI talent can be onboarded in days to avoid project delays. Stress quality by highlighting that hires are matched for technical skills and culture-fit, ensuring long-term performance. Include a personalized note referencing the recipient’s company, team, or AI initiatives if available.

# **If Salesforce-focused — **
# Showcase certified Salesforce developers, admins, and consultants across multiple clouds who can seamlessly integrate with the recipient’s Salesforce roadmap. Highlight flexible engagement models like contract, contract-to-hire, or remote staffing. Emphasize speed by noting that top Salesforce talent can be onboarded quickly to keep projects on track. Stress quality by mentioning that hires are matched for skills and cultural fit, ensuring continuous delivery. Include a personalized note referencing the recipient’s Salesforce setup, team, or current projects if data is available.

# **If both Salesforce + AI appear:**
# Present certified professionals skilled in both Salesforce and AI across multiple domains. Highlight engagement flexibility with contract, contract-to-hire, or remote staffing aligned to the recipient’s roadmap. Emphasize fast onboarding so top-tier talent can start contributing in days. Stress quality by noting hires are matched for both technical skills and culture-fit to maintain smooth project execution. Include a personalized reference to the recipient’s company, Salesforce use, or AI initiatives if available.

# **HUMAN THINKING MODE (CRITICAL):**
# Before writing the email, think like a real human sales professional:
# - Why would *you personally* reach out to this person?
# - What practical challenges are companies in this situation usually facing?
# - What simple, natural observation could start a real conversation?

# Write the email as if you are sending a personal note, not running a campaign.
# Slight unevenness in phrasing and rhythm is natural and encouraged


# **HUMANIZATION & SPAM-FREE RULES:**
# - Write in a natural, conversational human tone — never marketing or robotic.
# - Avoid buzzwords, hype, urgency triggers, and promotional language.
# - Prefer short, simple, direct sentences.
# - The email must feel like a genuine 1-to-1 note, not a bulk message.
# - No: “Hope you’re doing well”, “I hope this email finds you well”, or formal openers.
# - Use natural contractions (don’t, we’re, it’s).
# - Sound like a busy professional writing quickly and thoughtfully.
# - Do NOT sound like a company brochure or website.
# - If a sentence feels like marketing copy, rewrite it in simple spoken English.


# - **ZERO SIGNATURE:** Stop the email immediately after the Call to Action. Do not add "Best," "Regards," or "[Your Name]."

# **SERVICE FOCUS LOGIC:**
# - If the Target Role or Open Roles mention Salesforce, CRM, RevOps, Sales Ops, or Salesforce Developer → focus on Salesforce services.
# - If the Target Role or Open Roles mention AI, ML, Data, Automation, Engineering → focus on AI & automation services.
# - If both appear → blend Salesforce + AI naturally.

# **GUIDELINES:**

# - **Addressing (CRITICAL):**
#   Always greet using ONLY the first name of the contact.
#   If {ceo_name} contains a full name, extract and use only the first word.
#   Examples:
#   - "Kumar Sharma" → "Hi Kumar,"
#   - "John Michael Doe" → "Hi John,"
#   Never use last name, full name, Mr., Ms., or any title.
#   Format must always be: "Hi <FirstName>,"
# - **Read carefully:** Use the provided intelligence to reference their growth stage, focus areas, or recent activity.
# - **Interpret Hiring Volume:** Use the provided 'Open Roles Count' to frame their growth context:
#     - If **1 role**: Mention they are looking for a key {role}.
#     - If **2 roles**: Mention they are "expanding their team" with multiple {role}s.
#     - If **3 or more roles**: Mention they are "scaling their engineering team" with several {role} openings.
# - **Soft Personalization:** Do NOT invent facts. Use the data to make genuine observations.
# - **Pain Points:** Analyze the dataset to infer likely pain points (e.g., delivery bandwidth, scaling gaps, AI adoption, CRM maturity).
# - **Solution:** Position us as a strategic execution partner in **Salesforce + AI development**. Explain *how* we help (speed/efficiency), not just what we do.
# - **Tone:** Keep it concise, confident, human, and respectful.

# - **Structure You need to Strictly Follow:**
#     1. Short contextual opener showing genuine awareness (hook based on the Role or Market News and Financial intel).
#     2. Clear observation regarding their hiring volume (derived from the Open Roles logic above).
#     3. Value proposition in 2–3 bullet points tailored to their likely pain points.
#     4. **Strictly Follow**:- Short, low-friction CTA (10–15 minute chat), strictly aligned with service focus:
#         - If Salesforce-focused → mention only Salesforce support/services.
#         - If AI-focused → mention only AI / automation services.
#         - If both → mention Salesforce + AI together.
#         - The CTA must sound like a real person asking for time, not a sales pitch.
#             Example tone:
#             "Worth a quick 10–15 min chat to compare notes?"
#     5. DO NOT include signature or [Your Name] in the End.

# **ANTI-AI DETECTION RULE (CRITICAL):**
# Avoid:
# - Perfect symmetry
# - Over-polished structure
# - Corporate phrasing
# - Predictable patterns
# Allow:
# - Natural rhythm
# - Slight phrasing variation
# - Human sentence flow


# **STRICT OUTPUT FORMAT:**
# You must output the response in this exact format with the separators (The code depends on this):

# SUBJECT: [Relevant and slightly urgent subject line, not salesy]
# BODY_START
# [Insert Email Body Here]
# BODY_END
# """ 

    prompt = f"""
You are a senior B2B sales copywriter who specializes in writing highly personalized, one-to-one outbound emails for technology and consulting companies.

Your goal is to write emails that feel personalized, genuinely human, thoughtful, natural, and manually written — never templated, robotic, or marketing-heavy.

------------------------------------------------------------
DATA CONTEXT
------------------------------------------------------------
- Company Name: {company}
- Target Role (They are hiring): {role}
- Target Contact (Name): {ceo_name}
- Financial Intel: {financials}
- Market News: {market_news}
- Open Roles Count: {open_roles}

------------------------------------------------------------
SERVICE FOCUS LOGIC (Decide First Before Writing)
------------------------------------------------------------
- If Target Role or Open Roles mention Salesforce, CRM, RevOps, Sales Ops, or Salesforce Developer → Focus on Salesforce services.
- If Target Role or Open Roles mention AI, ML, Data, Automation, Engineering → Focus on AI & Automation services.
- If both appear → Blend Salesforce + AI naturally.

------------------------------------------------------------
OUR SERVICE DETAILS (Use strictly based on role relevance)
------------------------------------------------------------

If AI / Data / Automation-focused:
Highlight certified AI, data science, ML, and automation experts who are screened, tested, and ready-to-work.
Emphasize flexible hiring models (contract, contract-to-hire, remote staffing).
Highlight fast onboarding in days.
Stress technical match + culture-fit.


If Salesforce-focused:
Showcase certified Salesforce developers, admins, consultants across multiple clouds.
Highlight flexible engagement models (contract, contract-to-hire, remote staffing).
Emphasize quick onboarding.
Stress skills match + culture-fit.


If both Salesforce + AI appear:
Present certified professionals skilled in both.
Highlight engagement flexibility.
Emphasize fast onboarding.
Stress dual technical + culture alignment.


------------------------------------------------------------
HUMAN THINKING MODE (CRITICAL)
------------------------------------------------------------
Before writing, think like a real human sales professional:
- Why would you personally reach out?
- What practical challenges are companies in this situation facing?
- What natural observation could start a real conversation?

Write like you are sending a personal note — not running a campaign.
Slight unevenness in rhythm is natural and encouraged.

------------------------------------------------------------
HUMANIZATION & SPAM-FREE RULES
------------------------------------------------------------
- Use natural, conversational tone.
- Avoid buzzwords, hype, urgency triggers, promotional language.
- Prefer short, simple, direct sentences.
- No robotic or marketing tone.
- No: “Hope you’re doing well” or formal openers.
- Use contractions (don’t, we’re, it’s).
- Sound like a busy professional writing quickly.
- Do NOT sound like a brochure.
- If a sentence feels like marketing copy, simplify it.
------------------------------------------------------------
SAFE HIRING TRANSITION (CRITICAL)
------------------------------------------------------------
When referencing their Open Roles, strictly follow this logic:
1.  **State the Fact:** Mention the role they are hiring for.
2.  **Do NOT Assume Strategy:** Never use phrases like "implies you are fixing..." or "suggests you are struggling with..."
3.  **Offer Support/Capacity:** Immediately bridge to how we provide talent/bandwidth to help them.

**Use ONLY these types of generic transitions:**
- "As you look to fill this key [Role], I wanted to see if we can support your delivery bandwidth..."
- "Given that you're currently looking for a [Role], it's clear that finding the right talent is a priority..."
- "With the search for a [Role] underway, I wanted to reach out regarding immediate support..."

**Logic Flow:**
[Observation of Hiring] -> [Generic "Support" Statement] -> [Value Proposition]

**ZERO SIGNATURE**:
Stop immediately after the CTA.
Do NOT add “Best,” “Regards,” or any name.

------------------------------------------------------------
PERSONALIZATION & CONTEXT RULES
------------------------------------------------------------

Addressing (CRITICAL):
- Always greet using ONLY the first name.
- Extract first word from {ceo_name}.
- Examples:
    "Kumar Sharma" → "Hi Kumar,"
    "John Michael Doe" → "Hi John,"
- Never use last name, title, Mr., Ms.
- Format must be exactly: Hi <FirstName>,

Read Carefully:
Use provided intelligence to reference growth stage, focus areas, or activity.
Do NOT invent facts.

Interpret Hiring Volume:
- If 1 role → Mention they are hiring a key {role}.
- If 2 roles → Mention they are expanding their team with multiple {role}s.
- If 3+ roles → Mention they are scaling their engineering team with several {role} openings.

Soft Personalization:
Use only provided data.
Infer likely pain points (delivery bandwidth, scaling gaps, AI adoption, CRM maturity).

Solution Positioning:
Position us as a strategic execution partner in Salesforce + AI development.
Explain HOW we help (speed, efficiency) — not just WHAT we do.

Tone:
Concise. Confident. Human. Respectful.


------------------------------------------------------------
STRUCTURE (STRICTLY FOLLOW)
------------------------------------------------------------
1. Short contextual opener (hook based on Role, Market News, or Financial intel).
 - Always begin the email using the company’s most recent News, Market Updates, or Financial Updates. This must be    strictly followed. Do not start the email with hiring references or generic observations if recent company updates are available. The opening line must be directly anchored to the latest News, Market, or Financial development provided in the data
2. Clear observation about hiring volume.
AVOID:
- Obvious assumptions
- Direct hiring statements
- Overly confident claims about their internal challenges
Instead:
Frame hiring need as a reasonable business implication,
not a confirmed fact.

3. Value Bridge & Bullets: 
   - STRICT INSTRUCTION:When introducing bullet points, begin with a natural introductory phrase similar in  tone and structure to:
    "Here are some ways we can help:"
    However:
    - Do NOT use this exact sentence.
    - Create a variation that conveys the same meaning.
    - Keep it professional and aligned with the email tone.
    - Use a natural variation that fits a one-to-one B2B email.

   - Follow with 3-4 bullet points that describe SOLUTIONS and BENEFITS (do not list their problems).

4. Short, low-friction CTA (10–15 minute chat), aligned with service focus:
    - Salesforce-focused → Mention only Salesforce.
    - AI-focused → Mention only AI / automation.
    - Both → Mention Salesforce + AI.
    CTA must sound human.
 
5. No signature.

------------------------------------------------------------
ANTI-AI DETECTION RULE (CRITICAL)
------------------------------------------------------------
Avoid:
- Perfect symmetry
- Over-polished structure
- Corporate phrasing
- Predictable patterns

Allow:
- Natural rhythm
- Slight phrasing variation
- Human sentence flow

------------------------------------------------------------
STRICT OUTPUT FORMAT (DO NOT CHANGE)
------------------------------------------------------------
You must output exactly:

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
                    temperature=0.8,
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