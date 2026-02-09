# import gspread
# import requests
# import os
# import json
# from dotenv import load_dotenv
# load_dotenv()
 
# INSTANTLY_API_KEY = os.getenv("INSTANTLY_API_KEY")
# INSTANTLY_CAMPAIGN_ID = os.getenv("INSTANTLY_CAMPAIGN_ID")
# GOOGLE_SHEET_NAME = "Email_tool"
# GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
 
# def get_gspread_client():
#     if not GOOGLE_SERVICE_ACCOUNT_JSON:
#         raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not found in environment")
 
#     creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
#     return gspread.service_account_from_dict(creds_dict)
 
# def read_leads_from_sheet(sheet_name):
#     gc = get_gspread_client()
#     sheet = gc.open(sheet_name).sheet1
#     rows = sheet.get_all_records()
 
#     leads = []
#     for row in rows:
#         if not row.get("Email ID"):
#             continue
 
#         leads.append({
#             "email": row["Email ID"],
#             "subject": row.get("Email Subject", ""),
#             "body": row.get("Email Body", "")
#         })
 
#     return leads
 
# def send_to_instantly(leads):
#     url = "https://api.instantly.ai/api/v2/leads"
 
#     headers = {
#         "Authorization": f"Bearer {INSTANTLY_API_KEY}",
#         "Content-Type": "application/json",
#     }
 
#     results = []
 
#     for lead in leads:
#         payload = {
#             "email": lead["email"],
#             "campaign": INSTANTLY_CAMPAIGN_ID,
#             "add_to_campaign": True,
#             "custom_variables": {
#                 "subject": lead["subject"],
#                 "body": lead["body"],
 
#             }
#         }
 
#         response = requests.post(url, headers=headers, json=payload)
 
#         print("EMAIL:", lead["email"])
#         print("STATUS:", response.status_code)
#         print("RESPONSE:", response.text)
#         print("-" * 50)
 
#         if response.status_code not in (200, 201):
#             continue
 
#         results.append(response.json())
 
#         # time.sleep(0.3)  # ✅ avoid rate limits
 
#     return results
 
 
# leads = read_leads_from_sheet(GOOGLE_SHEET_NAME)
# result = send_to_instantly(leads)
# print(result)


import gspread
import requests
import os
import json
from dotenv import load_dotenv
load_dotenv()
 
INSTANTLY_API_KEY = os.getenv("INSTANTLY_API_KEY")
INSTANTLY_CAMPAIGN_ID = os.getenv("INSTANTLY_CAMPAIGN_ID")
GOOGLE_SHEET_NAME = "Email_tool"
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
 
def get_gspread_client():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not found in environment")
 
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    return gspread.service_account_from_dict(creds_dict)
 
def read_leads_from_sheet(sheet_name):
    gc = get_gspread_client()
    sheet = gc.open(sheet_name).sheet1
    rows = sheet.get_all_records()
 
    leads = []
    for row in rows:
        if not row.get("Email ID"):
            continue
 
        leads.append({
            "email": row["Email ID"],
            "subject": row.get("Email Subject", ""),
            "body": row.get("Email Body", "")
        })
 
    return leads
 
def send_to_instantly(leads):
    url = "https://api.instantly.ai/api/v2/leads/add"
 
    headers = {
        "Authorization": f"Bearer {INSTANTLY_API_KEY}",
        "Content-Type": "application/json",
    }
 
    payload = {
        "campaign_id": INSTANTLY_CAMPAIGN_ID,
        "leads": []
    }
 
    for lead in leads:
        payload["leads"].append({
            "email": lead["email"],
            "custom_variables": {
                "subject": lead["subject"],
                "body": lead["body"],
            }
        })
 
    response = requests.post(url, headers=headers, json=payload)
 
    print("STATUS:", response.status_code)
    print("RESPONSE:", response.text)
 
    response.raise_for_status()
    return response.json()
 
def activate_campaign(campaign_id):
    url = f"https://api.instantly.ai/api/v2/campaigns/{campaign_id}/activate"
 
    headers = {
        "Authorization": f"Bearer {INSTANTLY_API_KEY}",
        "Content-Type": "application/json",
    }
 
    response = requests.post(url, headers=headers, json={})
 
    print("ACTIVATE STATUS:", response.status_code)
    print("ACTIVATE RESPONSE:", response.text)
 
    response.raise_for_status()
    return response.json()
 
 
 
# leads = read_leads_from_sheet(GOOGLE_SHEET_NAME)
# result = send_to_instantly(leads)
sahil=activate_campaign(INSTANTLY_CAMPAIGN_ID)
