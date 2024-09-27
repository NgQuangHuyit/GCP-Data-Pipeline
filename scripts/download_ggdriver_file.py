import argparse
import time
import io
import os

from googleapiclient.discovery import build
from google.oauth2 import service_account
import logging
from googleapiclient.http import MediaIoBaseDownload
import threading


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class GGDriveDownloader:
    def __init__(self, creds, root_folder_id):
        self.creds = creds
        self.common_drive_service = build('drive', 'v3', credentials=self.creds)
        self.root_folder_id = root_folder_id
        self.logger = logging.getLogger(__class__.__name__)
        self.lock = threading.Lock()

    def list_subfolders(self, folder_id):
        results = self.common_drive_service.files().list(
            q=f"'{folder_id}' in parents",
            fields="files(id, name)"
        ).execute()
        items = results.get('files', [])
        return items

    def download_file(self, file_name, file_id, output_path):
        self.logger.info(f"Downloading {file_name}...")
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        request = build('drive', 'v3', credentials=self.creds).files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        retries = 5
        while not done:
            try:
                status, done = downloader.next_chunk()
                self.logger.info(f"Download {int(status.progress() * 100)}%.")
            except TimeoutError as e:
                if retries > 0:
                    retries -= 1
                    wait_time = (5 - retries) * 2  # Exponential backoff
                    self.logger.warning(f"TimeoutError: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Failed to download {file_name} after multiple retries.")
                    return
        with self.lock:
            with open(f"{output_path}/{file_name}", 'wb') as f:
                f.write(fh.getvalue())
        fh.close()
        self.logger.info(f"{file_name} downloaded.")

    def multiThreadDownloadFiles(self, folder_id, output_folder):
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        files = self.list_subfolders(folder_id)
        threads = []
        for file in files:
            thread = threading.Thread(target=self.download_file, args=(file["name"], file["id"], output_folder))
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.logger.info("All files downloaded.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download files from Google Drive')
    parser.add_argument('--execution-date', type=str, help='Execution date')
    parser.add_argument('--credential-file', type=str, help='Service account file')
    parser.add_argument('--gg-drive-folder-id', type=str, help='Google Drive folder ID')
    parser.add_argument('--target-dir', type=str, help='Output folder')

    args = parser.parse_args()
    execution_date = args.execution_date
    credential_file = args.credential_file
    GG_DRIVE_FOLDER_ID = args.gg_drive_folder_id
    target_dir = args.target_dir

    creds = service_account.Credentials.from_service_account_file(credential_file)
    downloader = GGDriveDownloader(creds, GG_DRIVE_FOLDER_ID)
    subfolders = downloader.list_subfolders(GG_DRIVE_FOLDER_ID)
    to_day_folder = None
    for folder in subfolders:
        if folder['name'] == execution_date:
            to_day_folder = folder
            break
    if to_day_folder:
        downloader.multiThreadDownloadFiles(to_day_folder['id'], f"{target_dir}/{execution_date}")
    else:
        logging.error(f"Folder {execution_date} not found.")

