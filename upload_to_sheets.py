



import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
from dotenv import load_dotenv
from gspread.utils import rowcol_to_a1

# --- CONFIGURATION ---
load_dotenv()
BASE_DIR = Path(__file__).resolve().parent
GOOGLE_SHEET_NAME = "Company_data"

# Variable get karo
SERVICE_ACCOUNT_RAW = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
# Render wala variable (Optional)
JSON_CONTENT_RENDER = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT")

MAX_CELL_CHARS = 49000

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def truncate_cell(value):
    if isinstance(value, str) and len(value) > MAX_CELL_CHARS:
        return value[:MAX_CELL_CHARS] + "… [TRUNCATED]"
    return value

def flatten_json(data, parent_key="", sep="_"):
    items = {}
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, sep))
        elif isinstance(value, list):
            cleaned_list = []
            for v in value:
                if isinstance(v, dict):
                    cleaned_list.append("; ".join(f"{k}:{str(val)}" for k, val in v.items()))
                else:
                    cleaned_list.append(str(v))
            items[new_key] = " | ".join(cleaned_list)
        else:
            items[new_key] = value
    return items

def get_gspread_client():
    """
    Ye function SMART hai. Ye check karega ki aapne 'File ka Path' diya hai
    ya 'Direct JSON Code' diya hai, aur dono ko handle karega.
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    creds = None

    # 1. Check Render Environment Variable first
    if JSON_CONTENT_RENDER:
        try:
            creds_dict = json.loads(JSON_CONTENT_RENDER)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        except json.JSONDecodeError:
            print(" Error: GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT is not valid JSON.")

    # 2. Check Standard Variable (Path or JSON)
    elif SERVICE_ACCOUNT_RAW:
        # CASE A: Agar ye '{' se shuru ho raha hai, to ye JSON Text hai
        if SERVICE_ACCOUNT_RAW.strip().startswith("{"):
            try:
                creds_dict = json.loads(SERVICE_ACCOUNT_RAW)
                creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            except json.JSONDecodeError:
                raise ValueError(" GOOGLE_SERVICE_ACCOUNT_JSON contains invalid JSON.")
        
        # CASE B: Agar ye normal text hai, to ye File Path hai
        else:
            if not os.path.exists(SERVICE_ACCOUNT_RAW):
                 raise FileNotFoundError(f" Credential file not found at: {SERVICE_ACCOUNT_RAW}")
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_RAW, scopes=scopes)
            
    if not creds:
        raise ValueError(" No valid Google Credentials found in Environment Variables.")
    
    return gspread.authorize(creds)

# ============================================================
# CORE UPLOAD LOGIC (BATCH PROCESSING)
# ============================================================

# def upload_batch_data(file_paths_list):
#     if not file_paths_list:
#         return

#     try:
#         # 1. Connect to Sheet
#         client = get_gspread_client()
#         sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        
#         # 2. Prepare Data from Files
#         new_rows_data = []
#         for file_path in file_paths_list:
#             if not file_path.exists(): continue
#             try:
#                 with file_path.open("r", encoding="utf-8") as f:
#                     data = json.load(f)
#                 new_rows_data.append(flatten_json(data))
#             except Exception as e:
#                 print(f" Skipping corrupt file {file_path.name}: {e}")
        
#         if not new_rows_data: return

#         # 3. Convert to DataFrame
#         df_new = pd.DataFrame(new_rows_data)
#         # df_new = df_new.fillna("").applymap(truncate_cell)
#         df_new = df_new.fillna("").map(truncate_cell)

#         # 4. Handle Headers
#         existing_data = sheet.get_all_records()
#         existing_df = pd.DataFrame(existing_data)
        
#         if existing_df.empty:
#             sheet.update([df_new.columns.tolist()] + df_new.values.tolist())
#             print(f" Initialized Sheet with {len(df_new)} rows.")
#             return

#         # 5. Row-by-Row Upsert Logic
#         try:
#             existing_companies = sheet.col_values(1) 
#         except:
#             existing_companies = []

#         headers = sheet.row_values(1)
        
#         for index, row in df_new.iterrows():
#             company_name = row.get("company_profile_company_name", "Unknown")
            
#             # Organize data to match the sheet's column order
#             # row_values_ordered = [row.get(h, "") for h in headers] 

#             # if company_name in existing_companies:
#             #     # UPDATE Existing Row
#             #     row_idx = existing_companies.index(company_name) + 1
#             #     sheet.update(f"A{row_idx}", [row_values_ordered])
#             # else:
#             #     # APPEND New Row
#             #     sheet.append_row(row_values_ordered)


#             if company_name in existing_companies:
#                 # UPDATE ONLY PROVIDED COLUMNS (PATCH UPDATE)
#                 row_idx = existing_companies.index(company_name) + 1

#                 updates = []

#                 for key, value in row.items():
#                     # update only if column exists AND value is not empty
#                     if key in headers and value != "":
#                         col_idx = headers.index(key) + 1
#                         cell = rowcol_to_a1(row_idx, col_idx)
#                         updates.append({
#                             "range": cell,
#                             "values": [[value]]
#                         })

#                 if updates:
#                     sheet.batch_update(updates)

#             else:
#                 # APPEND NEW ROW (same as before)
#                 row_values_ordered = [row.get(h, "") for h in headers]
#                 sheet.append_row(row_values_ordered)


#         print(f" Batch Upload Complete: Processed {len(file_paths_list)} files.")

#     except Exception as e:
#         print(f" Batch Upload Failed: {e}")







def upload_batch_data(file_paths_list):
    if not file_paths_list:
        return
 
    try:
        # 1. Connect to Sheet
        client = get_gspread_client()
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
       
        # 2. Prepare Data from Files
        new_rows_data = []
        for file_path in file_paths_list:
            if not file_path.exists(): continue
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                new_rows_data.append(flatten_json(data))
            except Exception as e:
                print(f"⚠️ Skipping corrupt file {file_path.name}: {e}")
       
        if not new_rows_data: return
 
        # 3. Convert to DataFrame
        df_new = pd.DataFrame(new_rows_data)
        # df_new = df_new.fillna("").applymap(truncate_cell)
        df_new = df_new.fillna("").map(truncate_cell)
 
        # 4. Handle Headers
        existing_data = sheet.get_all_records()
        existing_df = pd.DataFrame(existing_data)
       
        if existing_df.empty:
            sheet.update([df_new.columns.tolist()] + df_new.values.tolist())
            print(f"✅ Initialized Sheet with {len(df_new)} rows.")
            return
 
        # 5. Row-by-Row Upsert Logic
        try:
            existing_companies = sheet.col_values(1)
        except:
            existing_companies = []
 
        headers = sheet.row_values(1)
       
        for index, row in df_new.iterrows():
            company_name = row.get("company_profile_company_name", "Unknown")
           
            # Organize data to match the sheet's column order
            row_values_ordered = [row.get(h, "") for h in headers]
 
            if company_name in existing_companies:
                # UPDATE Existing Row (NON-DESTRUCTIVE)
 
                row_idx = existing_companies.index(company_name) + 1
 
                # 1️⃣ Fetch existing row values
                existing_row_values = sheet.row_values(row_idx)
 
                # 2️⃣ Pad existing row if shorter than headers
                if len(existing_row_values) < len(headers):
                    existing_row_values += [""] * (len(headers) - len(existing_row_values))
 
                # 3️⃣ Merge: keep old values if new is empty
                merged_row = []
                for i, header in enumerate(headers):
                    new_val = row.get(header, "")
                    old_val = existing_row_values[i]
                    merged_row.append(new_val if new_val != "" else old_val)
 
                # 4️⃣ Update merged row
                sheet.update(f"A{row_idx}", [merged_row])
            else:
                # APPEND New Row
                sheet.append_row(row_values_ordered)
 
        print(f"✅ Batch Upload Complete: Processed {len(file_paths_list)} files.")
 
    except Exception as e:
        print(f"❌ Batch Upload Failed: {e}")