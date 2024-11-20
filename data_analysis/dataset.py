from pathlib import Path
from loguru import logger
from tqdm import tqdm
import kaggle
import pandas as pd

from data_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR, EXTERNAL_DATA_DIR


class DataDownloader:
    def __init__(self, dataset: str, raw_data_dir: Path, processed_data_dir: Path):
        self.dataset = dataset
        self.raw_data_dir = raw_data_dir
        self.processed_data_dir = processed_data_dir

    def download_data(self):
        """Download and extract dataset from Kaggle."""
        logger.info(f"Starting download for dataset '{self.dataset}'...")
        try:
            self.raw_data_dir.mkdir(parents=True, exist_ok=True)
            kaggle.api.dataset_download_files(
                self.dataset,
                path=str(self.raw_data_dir),
                unzip=True,
            )
            logger.success(f"Dataset downloaded and extracted to: {self.raw_data_dir}")
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")


class DataClean:
    def __init__(self, external_input_data: Path, output_data: Path):
        self.inputdata = external_input_data
        self.outputdata = output_data

    def show_data_before(self, rows: int = 3):
        """Display the first few rows of a specified CSV file."""
        input_path = self.inputdata
        try:
            data = pd.read_csv(input_path)
            print(data.head(rows))
        except Exception as e:
            logger.error(f"Failed to read data: {e}")

    def show_data_after(self, filename: str, rows: int = 3):
        """Display the first few rows of a specified CSV file."""
        input_path = self.outputdata / filename
        try:
            data = pd.read_csv(input_path)
            print(data.head(rows))
        except Exception as e:
            logger.error(f"Failed to read data: {e}")


# Example usage:
if __name__ == "__main__":
    DATASET_NAME = "arshkon/linkedin-job-postings"

    downloader = DataDownloader(
        dataset=DATASET_NAME,
        raw_data_dir=RAW_DATA_DIR,
        processed_data_dir=PROCESSED_DATA_DIR,
    )
    downloader.download_data()

    dataclean = DataClean(
        external_input_data=EXTERNAL_DATA_DIR / "postings.csv",
        output_data=PROCESSED_DATA_DIR,
    )
    dataclean.show_data_before()

    dataclean.show_data_after()
