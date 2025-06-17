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

    for tag in order.get("tags", "").split(","):
        tag = tag.strip()

        # Find date anywhere in the tag (e.g., "11-18-2021, 14:00, Duval")
        date_match = re.search(r"\d{2}[-/]\d{2}[-/]\d{4}", tag)
        if date_match:
            raw_date = date_match.group().replace('/', '-')
            try:
                tag_date = datetime.strptime(raw_date, "%m-%d-%Y")
                break
            except ValueError:
                continue

    if not tag_date:
        return False

    # Check if date is within 31 days of today
    return abs((datetime.now() - tag_date).days) <= 31



def format_order_row(order):
    # === Parse note attributes ===
    attrs = {a['name']: a['value'] for a in order.get('note_attributes', [])}

    # === Extract customer name and shipping name ===
    customer = order.get('customer') or {}
    customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip()

    shipping = order.get('shipping_address') or {}
    shipping_name = shipping.get('name', '')

    # === Extract and format line items ===
    line_items = order.get('line_items', [])
    items = [f"{li['quantity']} x {li['title']}" for li in line_items if li.get('name', '').strip().upper() != "TIP"]

    # === Extract financial and fulfillment status ===
    financial_status = order.get('financial_status', '')
    fulfillment_status = order.get('fulfillment_status', '')

    # === Extract Tip Amount ===
    tip_item = next((li for li in line_items if li.get('name', '').strip().upper() == "TIP"), {})
    tip_amount = tip_item.get('pre_tax_price', '')

    # === Tray Identification Logic ===
    def is_tray(title):
        title = title.upper().strip()

        tray_exact_matches = {
            "CHEESE-Y DINNER FOR 2",
            "CHEESE + MEAT (L)",
            "CHEESE + MEAT (S)",
            "CHEESE + MEAT + TREATS (L)",
            "CHEESE + MEAT + TREATS (S)",
            "READY TO SERVE GRAZING TABLE (L) SERVES 50+",
            "READY TO SERVE GRAZING TABLE (S) SERVES 30+",
            "CHARCUTERIE \"CURED MEAT\" PLATTER",
            "CURED MEAT PLATTER",
            "BREAD & CRACKER PLATTER",
            "GLUTEN FREE CRACKER TRAY",
            "DESSERT TRAY",
            "THANKSGIVING CHEESE & CHARCUTERIE TRAY (L)",
            "THANKSGIVING CHEESE & CHARCUTERIE TRAY (S)",
            "HOLIDAY CHEESE & CHARCUTERIE TRAY (L)",
            "HOLIDAY CHEESE & CHARCUTERIE TRAY (S)",
            "VEGETARIAN CHEESE TRAY (L)",
            "VEGETARIAN CHEESE TRAY (S)",
            "PASTEURIZED CHEESE TRAY (L)",
            "PASTEURIZED CHEESE TRAY (S)",
            "VEGETARIAN & PASTEURIZED CHEESE TRAY (L)",
            "VEGETARIAN & PASTEURIZED CHEESE TRAY (S)",
            "EASTER TRAY (L)",
            "EASTER TRAY (S)",
            "FATHER'S DAY TRAY (L)",
            "FATHER'S DAY TRAY (S)",
            "MOTHER'S DAY TRAY (L)",
            "MOTHER'S DAY TRAY (S)",
            "CHAR-BOO-TERIE!  SPOOKY CHEESE + MEAT + TREATS (S) (10/24-10/31)",
            "CHAR-CUTE-TERIE!  HEART(Y) VALENTINE'S DAY CHEESE TRAY - MY LOVE DESERVES NOTHING LESS!",
            "CHAR-CUTE-TERIE!  HEART(Y) VALENTINE'S DAY CHEESE TRAY - NO, THANK YOU"
        }

        tray_partial_keywords = [
            "TRAY",
            "PLATTER",
            "GRAZING TABLE",
            "SNACK PACK"
        ]

        # Exact match
        if title in tray_exact_matches:
            return True

        # Keyword fallback
        return any(keyword in title for keyword in tray_partial_keywords)

    # === Separate trays and add-ons ===
    trays = [f"{li['quantity']} x {li['title']}" for li in line_items if is_tray(li['title'])]
    addons = [f"{li['quantity']} x {li['title']}" for li in line_items if not is_tray(li['title']) and li.get('name', '').strip().upper() != "TIP"]

    # === Determine delivery vs pickup ===
    is_delivery = 'Delivery-Location-Id' in attrs
    delivery_type = 'delivery' if is_delivery else 'pickup'
    time_str = attrs.get('Delivery-Time') if is_delivery else attrs.get('Pickup-Time') or 'N/A'
    date_str = attrs.get('Delivery-Date') or attrs.get('Pickup-Date') or ''

    # === Format shipping address ===
    address_lines = filter(None, [
        shipping.get('address1'),
        shipping.get('address2'),
        f"{shipping.get('city', '')}, {shipping.get('province_code', '')} {shipping.get('zip', '')}",
        "US",
        f"Phone: {shipping.get('phone', '')}"
    ])

    address_str = '  '.join(address_lines)

    # === Final formatted row ===
    return [
        str(order.get('id')),
        f"#{order.get('order_number')}",
        customer_name,
        shipping_name,
        '; '.join(trays),
        '; '.join(addons),
        date_str,
        time_str,
        order.get('total_price', ''),
        '',  # Placeholder for manual override column or similar
        attrs.get('Gift Note', ''),
        attrs.get('Special Requests', ''),
        attrs.get('Delivery-Location-Id') or attrs.get('Pickup-Location-Id'),
        delivery_type,
        address_str,
        attrs.get('Delivery Fee', ''),
        attrs.get('Favor Tag', ''),
        tip_amount,
        '; '.join(items),
        financial_status,
        fulfillment_status,
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

def prune_old_tabs(sheet_service, spreadsheet_id, valid_tab_names):
    """Delete tabs not in the valid list (usually date tabs within range)."""
    spreadsheet = sheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    requests = []

    for sheet in spreadsheet.get("sheets", []):
        title = sheet["properties"]["title"]
        sheet_id = sheet["properties"]["sheetId"]

        if title == "template":
            continue  # keep the template tab

        if title not in valid_tab_names:
            print(f"🗑️ Deleting old tab: {title}")
            requests.append({
                "deleteSheet": {
                    "sheetId": sheet_id
                }
            })

    if requests:
        sheet_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()


def main():
    print("Authenticating with Google Sheets...")
    creds = authenticate_with_oauth()
    sheet_service = build("sheets", "v4", credentials=creds)

    headers = [
    'Transaction ID', 'Order #', 'Customer Name', 'Shipping Name', 'Trays/Gifts', 'Add-ons',
    'Date', 'Time', 'Amount', 'Refunded', 'Gift Note', 'Special Requests', 'Location',
    'Pickup / Delivery', 'Address', 'Delivery Fee', 'Scheduled Delivery?', 'Tip Amount',
    'All Items', 'Financial Status', 'Fulfillment Status'
    ]
    rows_by_day = {}
    valid_tab_names = set()
    match_count = 0

    print("Filtering matching orders...")
    for order in fetch_shopify_orders_streaming():
        print(f"\n--- Order #{order['order_number']} Tags ---")
        print(order.get("tags", ""))

        if not matches_criteria(order):
            continue

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

        if date_str not in rows_by_day:
            rows_by_day[date_str] = [headers]
        rows_by_day[date_str].append(row)
        valid_tab_names.add(date_str)
        match_count += 1

    print(f"\nTotal matching orders: {match_count}")

    # Ensure all needed tabs exist before uploading
    print("\nEnsuring all tabs exist in order...")
    existing_tabs = sheet_service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    existing_titles = [s['properties']['title'] for s in existing_tabs['sheets']]

    for date_str in sorted(rows_by_day.keys()):
        if date_str not in existing_titles:
            print(f"  Creating tab: {date_str}")
            duplicate_template(sheet_service, SPREADSHEET_ID, date_str)

    # Upload all data to the correct tabs in sorted order
    print("\nUploading to Google Sheets...")
    for date_str in sorted(rows_by_day.keys(), key=lambda x: datetime.strptime(x, "%Y-%m-%d")):
        rows = rows_by_day[date_str]
        safe_upload(sheet_service, SPREADSHEET_ID, date_str, rows)
        time.sleep(2.5)

    # Prune any tabs outside the 30-day window
    print("\nPruning old tabs...")
    cutoff = datetime.now() - timedelta(days=30)
    recent_only = {d for d in valid_tab_names if datetime.strptime(d, "%Y-%m-%d") >= cutoff}
    prune_old_tabs(sheet_service, SPREADSHEET_ID, recent_only)

    print("✅ All done.")

if __name__ == "__main__":
    print("Running order sync...")
    main()
    print("Done.")