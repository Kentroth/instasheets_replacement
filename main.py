import os
import time
import requests
import pandas as pd
import urllib.parse
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import re

import os
import base64

# === Write the Google OAuth files from base64 env vars ===
if os.getenv("GOOGLE_CREDENTIALS_BASE64"):
    with open("client_secret_sheets.json", "wb") as f:
        f.write(base64.b64decode(os.getenv("GOOGLE_CREDENTIALS_BASE64")))

if os.getenv("GOOGLE_TOKEN_BASE64"):
    with open("sheets_token.json", "wb") as f:
        f.write(base64.b64decode(os.getenv("GOOGLE_TOKEN_BASE64")))

# === CONFIG ===
SHOP = 'antonellischeese.myshopify.com'  # Safe to hardcode
API_VERSION = '2024-04'

ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

CLIENT_SECRET_PATH = "client_secret_sheets.json"
TOKEN_PATH = "sheets_token.json"

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


# === AUTH ===
def authenticate_with_oauth():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    return creds

import time
from googleapiclient.errors import HttpError



# === FETCH SHOPIFY ORDERS ===
def fetch_shopify_orders_streaming():
    now = datetime.utcnow()
    start_date = now - timedelta(days=120)
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    headers = {
        'X-Shopify-Access-Token': ACCESS_TOKEN,
        'Content-Type': 'application/json',
    }

    url = f"https://{SHOP}/admin/api/{API_VERSION}/orders.json?updated_at_min={start_date_str}&limit=250&status=any"

    while url:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break

        data = response.json().get('orders', [])
        if not data:
            break

        for order in data:
            yield order  # YIELD one order at a time

        link_header = response.headers.get('Link', '')
        next_url = None
        for part in link_header.split(','):
            if 'rel="next"' in part:
                next_url = part[part.find('<') + 1:part.find('>')]
                next_url = urllib.parse.unquote(next_url)
                break
        url = next_url


# === FILTER & FORMAT ===
def matches_criteria(order):
    tag_date = None

    # Look for a tag matching MM-DD-YYYY
    for tag in order.get("tags", "").split(","):
        tag = tag.strip()
        if re.match(r"\d{2}-\d{2}-\d{4}", tag):
            try:
                tag_date = datetime.strptime(tag, "%m-%d-%Y")
                break
            except ValueError:
                continue

    if not tag_date:
        return False

    # Check if tag_date is within 31 days of today
    delta_days = abs((datetime.now() - tag_date).days)
    return delta_days <= 31


def format_order_row(order):
    attrs = {item['name']: item['value'] for item in order.get('note_attributes', [])}
    name = f"{order['customer'].get('first_name', '')} {order['customer'].get('last_name', '')}".strip()
    items = [f"{li['quantity']} x {li['title']}" for li in order['line_items']]
    trays = [i for i in items if 'DINNER' in i or 'TRAY' in i or 'GIFT' in i]
    addons = [i for i in items if i not in trays]

    address = order.get('shipping_address', {})
    address_str = f"{name}  {address.get('address1', '')}\n{address.get('address2', '')}\n{address.get('city', '')}, {address.get('province_code', '')} {address.get('zip', '')} US, Phone: {address.get('phone', '')}"

    is_delivery = 'Delivery-Location-Id' in attrs
    time_field = (
        attrs.get('Delivery-Time')
        if is_delivery else
        attrs.get('Pickup-Time') or 'N/A'
    )

    return [
        str(order['id']),  # ✅ Transaction ID fix here
        f"#{order['order_number']}",
        name,
        '; '.join(trays),
        '; '.join(addons),
        attrs.get('Delivery-Date') or attrs.get('Pickup-Date'),
        time_field,
        order.get('total_price', ''),
        '',
        attrs.get('Gift Note', ''),
        attrs.get('Special Requests', ''),
        attrs.get('Delivery-Location-Id') or attrs.get('Pickup-Location-Id'),
        'delivery' if is_delivery else 'pickup',
        address_str,
        attrs.get('Delivery Fee', ''),
        attrs.get('Favor Tag', ''),
        '; '.join(items)
    ]




# === UPLOAD TO SHEETS ===
def upload_to_sheet(sheet_service, sheet_id, sheet_name, rows):
    try:
        print(f"\nAttempting upload to tab: '{sheet_name}'")
        print(f"  Number of rows: {len(rows)}")

        # Check if sheet exists, create if not
        sheets_metadata = sheet_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheet_titles = [s['properties']['title'] for s in sheets_metadata['sheets']]
        if sheet_name not in sheet_titles:
            print(f"  Sheet '{sheet_name}' not found. Duplicating template...")
            duplicate_template(sheet_service, sheet_id, sheet_name)

        # Clear old values
        print(f"  Clearing existing contents of '{sheet_name}'...")
        sheet_service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=sheet_name
        ).execute()

        # Upload new data
        print(f"  Uploading new data to '{sheet_name}'...")
        sheet_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=sheet_name,
            valueInputOption='RAW',
            body={'values': rows}
        ).execute()
        print(f"✅ Upload successful to '{sheet_name}'.")

    except HttpError as err:
        print(f"❌ Error uploading to '{sheet_name}': {err}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

def safe_upload(sheet_service, sheet_id, date_str, rows, retries=3):
    for attempt in range(retries):
        try:
            upload_to_sheet(sheet_service, sheet_id, date_str, rows)
            return
        except HttpError as e:
            if "RATE_LIMIT_EXCEEDED" in str(e) or "Quota exceeded" in str(e):
                print(f"⚠️ Rate limit hit for {date_str}, retrying in 10 seconds...")
                time.sleep(10)
            else:
                raise

def duplicate_template(sheet_service, spreadsheet_id, new_title):
    template_sheet_id = None
    spreadsheet = sheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == 'template':
            template_sheet_id = sheet['properties']['sheetId']
            break

    if template_sheet_id is None:
        print("❌ Template tab not found.")
        return

    # Duplicate the template
    copied_sheet = sheet_service.spreadsheets().sheets().copyTo(
        spreadsheetId=spreadsheet_id,
        sheetId=template_sheet_id,
        body={"destinationSpreadsheetId": spreadsheet_id}
    ).execute()

    # Rename the duplicated sheet
    new_sheet_id = copied_sheet['sheetId']
    sheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [{
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": new_sheet_id,
                        "title": new_title
                    },
                    "fields": "title"
                }
            }]
        }
    ).execute()


# === MAIN ===
def main():
    print("Authenticating with Google Sheets...")
    creds = authenticate_with_oauth()
    sheet_service = build("sheets", "v4", credentials=creds)

    rows_by_day = {}
    headers = ['Transaction ID', 'Order #', 'Name', 'Trays/Gifts', 'Add-ons', 'Date', 'Time', 'Amount', 'Refunded', 'Gift Note', 'Special Requests', 'Location', 'Pickup / Delivery', 'Address', 'Delivery Fee', 'Scheduled Delivery?', 'All Items']

    print("Filtering matching orders...")
    match_count = 0
    for order in fetch_shopify_orders_streaming():
        print(f"\n--- Order #{order['order_number']} Tags ---")
        print(order.get("tags", ""))

        if matches_criteria(order):
            match_count += 1
            row = format_order_row(order)
            raw_date = row[5]

            if not raw_date:
                print(f"⚠️ Skipping order #{order['order_number']} due to missing date.")
                continue

            try:
                date_obj = datetime.strptime(raw_date.replace('/', '-'), "%Y-%m-%d")
            except ValueError:
                try:
                    date_obj = datetime.strptime(raw_date.replace('/', '-'), "%m-%d-%Y")
                except ValueError:
                    print(f"⚠️ Skipping order #{order['order_number']} due to invalid date format: {raw_date}")
                    continue

            date_str = date_obj.strftime("%Y-%m-%d")
            print(f"  ✓ Match: Order #{order['order_number']} for date {date_str}")

            # Upload this order immediately (includes header)
            safe_upload(sheet_service, SPREADSHEET_ID, date_str, [headers, row])




    print(f"Total matching orders: {match_count}")

    print("Uploading to Google Sheets...")
    for date_str in sorted(rows_by_day.keys(), key=lambda x: datetime.strptime(x, "%Y-%m-%d")):
        rows = rows_by_day[date_str]
        safe_upload(sheet_service, SPREADSHEET_ID, date_str, rows)
        time.sleep(2.5)




if __name__ == "__main__":
    print("Running order sync...")
    main()
    print("Done.")