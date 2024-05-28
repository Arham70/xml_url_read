import requests
import xml.etree.ElementTree as ET
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from io import BytesIO

# Function to fetch and parse XML from a URL
def fetch_and_parse_xml(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        xml_content = response.content
        tree = ET.ElementTree(ET.fromstring(xml_content))
        return tree
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch URL {url}: {e}")
        return None
    except ET.ParseError as e:
        print(f"Failed to parse XML from URL {url}: {e}")
        return None

# Function to rearrange XML data
def rearrange_xml(root):
    # Separate items based on the isfeatured field and condition field
    featured_items = []
    featured_n_items = []
    other_items = []

    for item in root.findall('item'):
        is_featured = item.find('isfeatured').text
        condition = item.find('condition').text

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

# Get all XML links from the second column and their corresponding row numbers
xml_links_and_rows = [(link, i) for i, link in enumerate(sheet.col_values(2)[1:], start=2)]  # Exclude header row

# Process each XML link and update the corresponding row with the URL of the file containing the rearranged XML content
for xml_link, row in xml_links_and_rows:
    xml_tree = fetch_and_parse_xml(xml_link)
    if xml_tree:
        root = xml_tree.getroot()

        # Rearrange XML data
        rearranged_items = rearrange_xml(root)

        # Create a new root element for the rearranged XML
        new_root = ET.Element("inventory")

        # Append rearranged items to the new root
        for item in rearranged_items:
            new_root.append(item)

        # Generate the new XML content
        new_xml_content = ET.tostring(new_root, encoding='utf-8')

        # Create a file in Google Drive
        file_metadata = {'name': f"rearranged_{row}.xml"}
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
