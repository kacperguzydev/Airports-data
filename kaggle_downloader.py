import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
from loguru import logger
from tracing import init_tracer

tracer = init_tracer()

DATASET_OWNER = "divyanshrai"
DATASET_NAME = "openflights-airports-database-2017"
CSV_FILENAME = "airports.csv"
DOWNLOAD_DIR = "./data"

def download_file():
    with tracer.start_as_current_span("download_file"):
        api = KaggleApi()
        api.authenticate()

        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        dataset_id = f"{DATASET_OWNER}/{DATASET_NAME}"

        file_path = api.dataset_download_file(
            dataset=dataset_id,
            file_name=CSV_FILENAME,
            path=DOWNLOAD_DIR,
            force=True
        )

        if not file_path or isinstance(file_path, bool):
            local_file = os.path.join(DOWNLOAD_DIR, CSV_FILENAME)
            local_zip_file = local_file + ".zip"
            if os.path.exists(local_file):
                file_path = local_file
            elif os.path.exists(local_zip_file):
                file_path = local_zip_file
            else:
                logger.error("Could not locate the downloaded file. Exiting.")
                return

        zip_path = os.path.join(DOWNLOAD_DIR, "airports.zip")
        os.rename(file_path, zip_path)
        logger.info("Renamed downloaded file to: {}", zip_path)

        if zipfile.is_zipfile(zip_path):
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(DOWNLOAD_DIR)
            os.remove(zip_path)
            logger.info("ZIP extracted and removed: {}", zip_path)
            logger.info("Extracted contents are now in '{}'.", DOWNLOAD_DIR)
        else:
            logger.info("File is not a valid ZIP archive. No extraction performed.")

if __name__ == "__main__":
    download_file()
