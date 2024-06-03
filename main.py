import requests
import xml.etree.ElementTree as ET
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from io import BytesIO
import time

# Function to fetch and parse XML from a URL with retries
def fetch_and_parse_xml(url, retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            xml_content = response.content
            tree = ET.ElementTree(ET.fromstring(xml_content))
            return tree
        except (requests.exceptions.RequestException, ET.ParseError) as e:
            print(f"Failed to fetch URL {url} on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                return None

# Function to rearrange XML data
def rearrange_xml(root):
    # Separate items based on the isfeatured field and condition field
    featured_items = []
    featured_n_items = []
    other_items = []

    for item in root.findall('item'):
        is_featured = item.find('isfeatured')
        condition = item.find('condition')

        # Ensure tags exist and extract text, defaulting to empty strings if missing
        is_featured = is_featured.text if is_featured is not None else ''
        condition = condition.text if condition is not None else ''

        if is_featured == '1':
            featured_items.append(item)
        elif condition == 'U':
            featured_n_items.append(item)
        else:
            other_items.append(item)

    # Rearrange items: featured items with condition N first, then others
    all_items = featured_items + featured_n_items + other_items

    return all_items

# Load the service account key JSON file
creds = Credentials.from_service_account_file(r"C:\Users\user\xml_url_read\client_secrets.json",
                                              scopes=["https://www.googleapis.com/auth/drive"])

# Authorize the client
creds.refresh(Request())
drive_service = build('drive', 'v3', credentials=creds)

# Open the spreadsheet by name
spreadsheet = gspread.service_account(filename=r"C:\Users\user\xml_url_read\client_secrets.json").open(
    "Copy of Copy of MaxOpp Product Feeds")

# Select the sheet named "Market Place"
sheet = spreadsheet.worksheet("Market Place")

# Fetch all data from the worksheet at once to avoid exceeding the quota
all_data = sheet.get_all_values()
xml_links_and_rows = [(row[0], row[1], i + 2) for i, row in enumerate(all_data[1:]) if len(row) >= 2]

# Function to check if file exists and return its ID
def get_file_id_by_name(name):
    page_token = None
    while True:
        response = drive_service.files().list(q="mimeType='application/xml'",
                                              spaces='drive',
                                              fields='nextPageToken, files(id, name)',
                                              pageToken=page_token).execute()
        for file in response.get('files', []):
            if file.get('name') == name:
                return file.get('id')
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return None

# Process each XML link and update the corresponding row with the URL of the file containing the rearranged XML content
for name, xml_link, row in xml_links_and_rows:
    # Introduce a delay before fetching the URL
    time.sleep(5)  # wait for 5 seconds

    xml_tree = fetch_and_parse_xml(xml_link)
    if xml_tree:
        root = xml_tree.getroot()

        # Rearrange XML data
        rearranged_items = rearrange_xml(root)

        if not rearranged_items:
            print(f"No items found for URL {xml_link} at row {row}")
            continue

        # Create a new root element for the rearranged XML
        new_root = ET.Element("inventory")

        # Append rearranged items to the new root
        for item in rearranged_items:
            new_root.append(item)

        # Generate the new XML content
        new_xml_content = ET.tostring(new_root, encoding='utf-8')

        # Check if file already exists on Google Drive
        file_id = get_file_id_by_name(f"{name}.xml")
        if file_id:
            # Update existing file
            media = MediaIoBaseUpload(BytesIO(new_xml_content), mimetype='application/xml', resumable=True)
            file = drive_service.files().update(fileId=file_id, media_body=media).execute()
        else:
            # Create a new file if it doesn't exist
            file_metadata = {'name': f"{name}.xml"}
            media = MediaIoBaseUpload(BytesIO(new_xml_content), mimetype='application/xml', resumable=True)
            file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

            # Set permission for all users to access
            drive_service.permissions().create(
                fileId=file['id'],
                body={'type': 'anyone', 'role': 'reader'}
            ).execute()

        # Get shareable link
        shareable_link = f"https://drive.google.com/file/d/{file['id']}/view?usp=sharing"

        # Update the corresponding row in the third column with the URL of the file
        sheet.update_cell(row, 3, shareable_link)

        # Print the updated XML URL
        print(f"Updated XML URL for row {row}: {shareable_link}")
    else:
        print(f"Failed to fetch or parse XML for URL {xml_link} at row {row}")
