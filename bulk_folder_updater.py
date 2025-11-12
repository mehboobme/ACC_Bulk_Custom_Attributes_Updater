import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import urllib.parse
import time

load_dotenv()

CLIENT_ID = os.getenv("APS_CLIENT_ID")
CLIENT_SECRET = os.getenv("APS_CLIENT_SECRET")
PROJECT_ID = os.getenv("PROJECT_ID")
ROOT_FOLDER_URN = os.getenv("ROOT_FOLDER_URN")

BASE_URL = "https://developer.api.autodesk.com"

# Attribute IDs from your ACC project
ATTRIBUTE_MAPPING = {
    "Package ID": 7374741,
    "Package Name": 7374744,
    "Contractor": 7374759,
    "Location": 7374769,
    "Planned Start": 7374777,
    "Planned End": 7374783,
    "Actual Start": 7374787,
    "Actual End": 7374795,
    "% Completion": 7374805
}


def get_token():
    """Get 2-legged OAuth token"""
    url = f"{BASE_URL}/authentication/v2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "data:read data:write data:create"
    }
    r = requests.post(url, headers=headers, data=data)
    r.raise_for_status()
    return r.json()["access_token"]


def list_folder_contents(token, project_id, folder_urn):
    """List all items in a folder"""
    url = f"{BASE_URL}/data/v1/projects/{project_id}/folders/{folder_urn}/contents"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return r.json().get("data", [])
        return []
    except:
        return []


def get_all_files_recursive(token, project_id, folder_urn, current_path=""):
    """Recursively get all files from folder and subfolders"""
    all_files = []
    
    contents = list_folder_contents(token, project_id, folder_urn)
    
    for item in contents:
        item_type = item.get("type")
        item_name = item.get("attributes", {}).get("displayName", "Unknown")
        item_id = item.get("id")
        
        if item_type == "items":
            # It's a file
            file_info = {
                "id": item_id,
                "name": item_name,
                "path": f"{current_path}/{item_name}" if current_path else item_name
            }
            all_files.append(file_info)
            print(f"   📄 Found: {file_info['path']}")
            
        elif item_type == "folders":
            # It's a subfolder - recurse into it
            subfolder_path = f"{current_path}/{item_name}" if current_path else item_name
            print(f"   📁 Scanning folder: {subfolder_path}")
            subfolder_files = get_all_files_recursive(token, project_id, item_id, subfolder_path)
            all_files.extend(subfolder_files)
    
    return all_files


def get_version_urn_from_item(token, project_id, item_urn):
    """Get the version URN from an item URN"""
    url = f"{BASE_URL}/data/v1/projects/{project_id}/items/{item_urn}/tip"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return r.json()["data"]["id"]
    except:
        pass
    return None


def update_custom_attributes(token, project_id, version_urn, attributes_payload):
    """Update custom attributes for a version"""
    project_id_without_b = project_id.replace("b.", "")
    encoded_version_urn = urllib.parse.quote(version_urn, safe='')
    
    url = f"{BASE_URL}/bim360/docs/v1/projects/{project_id_without_b}/versions/{encoded_version_urn}/custom-attributes:batch-update"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, headers=headers, json=attributes_payload)
        
        if response.status_code == 200:
            return True, "Success"
        else:
            return False, f"Status {response.status_code}"
    except Exception as e:
        return False, f"Exception: {str(e)}"


def format_date(date_value):
    """Format date to ISO8601"""
    if pd.isna(date_value) or date_value == "":
        return None
    
    try:
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y-%m-%dT00:00:00.000Z")
        
        date_str = str(date_value).strip()
        for fmt in ["%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%dT00:00:00.000Z")
            except ValueError:
                continue
        return None
    except:
        return None


def build_attributes_from_excel(excel_file, file_name, item_urn):
    """Build attributes payload for a specific file from Excel"""
    try:
        df = pd.read_excel(excel_file)
        
        # Try to match by file name (without extension)
        file_name_base = file_name.rsplit(".", 1)[0] if "." in file_name else file_name
        
        matched_row = None
        match_method = None
        
        # Strategy 1: Match by file_name column if it exists
        if "file_name" in df.columns:
            for _, row in df.iterrows():
                row_file_name = str(row.get("file_name", "")).strip()
                if row_file_name and (row_file_name == file_name or row_file_name == file_name_base):
                    matched_row = row
                    match_method = "file_name_column"
                    break
        
        # Strategy 2: Match by acc_file_id - check if it's a URN or just a filename
        if matched_row is None and "acc_file_id" in df.columns:
            for idx, row in df.iterrows():
                row_value = str(row.get("acc_file_id", "")).strip()
                
                if not row_value:
                    continue
                
                # Check if it's a URN (contains "urn:adsk")
                if "urn:adsk" in row_value or "dm.lineage" in row_value:
                    # It's a URN - compare with item URN
                    if not row_value.startswith("urn:"):
                        row_value = f"urn:{row_value}"
                    
                    if row_value == item_urn:
                        matched_row = row
                        match_method = "urn"
                        break
                else:
                    # It's a filename - compare with file name
                    row_file_base = row_value.rsplit(".", 1)[0] if "." in row_value else row_value
                    
                    if row_value == file_name or row_file_base == file_name_base or row_value == file_name_base:
                        matched_row = row
                        match_method = "filename_in_acc_file_id"
                        break
        
        # Strategy 3: If Excel has only 1 row, offer to apply to all files
        if matched_row is None and len(df) == 1:
            matched_row = df.iloc[0]
            match_method = "single_row"
        
        if matched_row is not None:
            # Found matching row - build payload
            payload = []
            for col in df.columns:
                if col in ATTRIBUTE_MAPPING:
                    value = matched_row.get(col)
                    
                    if "start" in col.lower() or "end" in col.lower():
                        value = format_date(value)
                    elif pd.notna(value):
                        value = str(value).strip()
                    else:
                        value = None
                    
                    if value:
                        payload.append({
                            "id": ATTRIBUTE_MAPPING[col],
                            "value": value
                        })
            
            return payload, match_method
        
        return None, "not_matched"
    except Exception as e:
        print(f"   ❌ Error reading Excel: {e}")
        import traceback
        traceback.print_exc()
        return None, "error"


def main():
    print("=" * 70)
    print("🔧 Bulk Update All Files in Folder")
    print("=" * 70)
    
    if not all([CLIENT_ID, CLIENT_SECRET, PROJECT_ID, ROOT_FOLDER_URN]):
        print("❌ Missing required environment variables")
        return
    
    excel_path = "acc_file_attributes.xlsx"
    use_excel = os.path.exists(excel_path)
    
    print("\n📋 Update Mode:")
    if use_excel:
        print(f"   ✅ Excel file found: {excel_path}")
        print("   Will apply attributes from Excel based on file name matching")
    else:
        print("   ⚠️  No Excel file found")
        print("   Will apply default attributes to all files")
    
    print(f"\n1️⃣  Authenticating...")
    token = get_token()
    print("✅ Authenticated")
    
    print(f"\n2️⃣  Scanning folder: {ROOT_FOLDER_URN}")
    all_files = get_all_files_recursive(token, PROJECT_ID, ROOT_FOLDER_URN)
    
    print(f"\n✅ Found {len(all_files)} file(s) total")
    
    if len(all_files) == 0:
        print("❌ No files found")
        return
    
    print("\n" + "=" * 70)
    choice = input(f"Update custom attributes for all {len(all_files)} file(s)? (y/n): ")
    if choice.lower() != 'y':
        print("Cancelled")
        return
    
    print("\n3️⃣  Updating files...")
    print("-" * 70)
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    results = []
    
    for idx, file_info in enumerate(all_files, 1):
        file_name = file_info["name"]
        item_urn = file_info["id"]
        file_path = file_info["path"]
        
        print(f"\n📄 [{idx}/{len(all_files)}] {file_path}")
        
        # Get attributes for this file
        if use_excel:
            payload, source = build_attributes_from_excel(excel_path, file_name, item_urn)
            if not payload:
                print(f"   ⏭️  Skipped - {source}")
                skipped_count += 1
                results.append({"file": file_path, "status": "skipped", "reason": source})
                continue
            print(f"   📊 Matched via {source} - applying attributes")
        else:
            print(f"   ⚠️  No Excel data - skipping")
            skipped_count += 1
            results.append({"file": file_path, "status": "skipped", "reason": "No Excel file"})
            continue
        
        # Get version URN
        version_urn = get_version_urn_from_item(token, PROJECT_ID, item_urn)
        
        if not version_urn:
            print(f"   ❌ Failed to get version URN")
            failed_count += 1
            results.append({"file": file_path, "status": "failed", "reason": "Version lookup failed"})
            continue
        
        # Update attributes
        success, message = update_custom_attributes(token, PROJECT_ID, version_urn, payload)
        
        if success:
            print(f"   ✅ Updated {len(payload)} attribute(s)")
            success_count += 1
            results.append({"file": file_path, "status": "success", "attrs": len(payload)})
        else:
            print(f"   ❌ Failed: {message}")
            failed_count += 1
            results.append({"file": file_path, "status": "failed", "reason": message})
        
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 Update Summary")
    print("=" * 70)
    print(f"✅ Success: {success_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"⏭️  Skipped: {skipped_count}")
    print(f"📁 Total: {len(all_files)}")
    
    # Save results
    results_df = pd.DataFrame(results)
    results_df.to_excel("bulk_update_results.xlsx", index=False)
    print(f"\n💾 Detailed results saved to: bulk_update_results.xlsx")
    
    print("=" * 70)


if __name__ == "__main__":
    main()