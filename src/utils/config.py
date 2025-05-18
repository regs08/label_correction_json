import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import BaseModel
from typing import Optional, List, Tuple
from pathlib import Path

# Load environment variables
load_dotenv()

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    azure_storage_connection_string: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    azure_source_container_name: str = os.getenv("AZURE_SOURCE_CONTAINER_NAME", "")
    azure_destination_container_name: str = os.getenv("AZURE_DESTINATION_CONTAINER_NAME", "")
    ground_truth_csv_path: str = os.getenv("GROUND_TRUTH_CSV_PATH", "data/ground_truth/ground_truth.csv")
    
    class Config:
        env_file = ".env"

class SetupConfig(BaseModel):
    """Configuration for folder structure and session management."""
    working_folder: str = "data"
    session_folder: str = "session"
    checkpoints_folder: str = "checkpoints"
    ground_truth_folder: str = "ground_truth"

    def get_session_path(self) -> Path:
        """Get the full path to the session folder."""
        return Path(self.working_folder) / self.session_folder

    def get_checkpoints_path(self) -> Path:
        """Get the full path to the checkpoints folder."""
        return Path(self.working_folder) / self.session_folder / self.checkpoints_folder

    def get_ground_truth_path(self) -> Path:
        """Get the full path to the ground truth folder."""
        return Path(self.working_folder) / self.ground_truth_folder

    def create_folders(self) -> None:
        """Create all necessary folders if they don't exist."""
        self.get_session_path().mkdir(parents=True, exist_ok=True)
        self.get_checkpoints_path().mkdir(parents=True, exist_ok=True)
        self.get_ground_truth_path().mkdir(parents=True, exist_ok=True)

class FileMatchConfig(BaseModel):
    """Configuration for matching files between source and ground truth folders."""
    source_folder: Path
    ground_truth_folder: Path
    output_folder: Optional[Path] = None
    recursive: bool = True  # Whether to search subdirectories

    def get_source_files(self) -> list[Path]:
        """Get all files from source folder."""
        if self.recursive:
            return list(self.source_folder.rglob("*"))
        return list(self.source_folder.glob("*"))

    def get_ground_truth_files(self) -> list[Path]:
        """Get all files from ground truth folder."""
        if self.recursive:
            return list(self.ground_truth_folder.rglob("*"))
        return list(self.ground_truth_folder.glob("*"))

    def get_output_path(self, source_file: Path) -> Path:
        """Get the output path for a matched file."""
        if self.output_folder is None:
            return source_file
        # Preserve the relative path structure from source
        rel_path = source_file.relative_to(self.source_folder)
        return self.output_folder / rel_path

class AzureDownloadConfig(BaseModel):
    """Configuration for Azure download flow."""
    prefix: Optional[str] = None
    output_dir: str = "data/output"

class CompareFilesConfig(BaseModel):
    """Configuration for comparing JSON labels with ground truth CSV files."""
    matched_files: List[Tuple[Path, Path]]  # List of (json_path, csv_path) tuples
    output_folder: Optional[Path] = None
    report_folder: Optional[Path] = None

    def get_output_path(self, json_path: Path) -> Path:
        """Get the output path for a corrected JSON file."""
        if self.output_folder is None:
            return json_path.parent / json_path.name
        return self.output_folder / json_path.name

    def get_report_path(self, json_path: Path) -> Path:
        """Get the output path for a correction report CSV."""
        if self.report_folder is None:
            return json_path.parent / f"{json_path.stem}.csv"
        return self.report_folder / f"{json_path.stem}.csv"

class UploadLabelsConfig(BaseModel):
    """Configuration for uploading label files to Azure."""
    source_folder: Path
    prefix: Optional[str] = None
    file_pattern: str = "*.labels.json"
    
    def get_source_files(self) -> List[Path]:
        """Get all matching files from source folder."""
        if not self.source_folder.exists():
            raise ValueError(f"Source directory does not exist: {self.source_folder}")
        return list(self.source_folder.glob(self.file_pattern))

class PipelineConfig(BaseModel):
    """High-level configuration that holds all other configs."""
    setup: SetupConfig
    azure_download: AzureDownloadConfig
    file_match: Optional[FileMatchConfig] = None
    compare_files: Optional[CompareFilesConfig] = None
    upload_labels: Optional[UploadLabelsConfig] = None

# Create a global settings instance
settings = Settings()

def get_settings() -> Settings:
    """Return the application settings."""
    return settings 