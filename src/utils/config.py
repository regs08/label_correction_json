import os
from dotenv import load_dotenv
from pathlib import Path
from typing import List

# Load environment variables
load_dotenv()

class Settings:
    """Application settings."""
    def __init__(
        self,
        input_csv_path: Path,
        input_json_path: Path,
        output_json_path: Path,
        schema_prefix: str = "schema",
        default_page: int = 1,
        required_csv_columns: List[str] = None
    ):
        self.input_csv_path = input_csv_path
        self.input_json_path = input_json_path
        self.output_json_path = output_json_path
        self.schema_prefix = schema_prefix
        self.default_page = default_page
        self.required_csv_columns = required_csv_columns or [
            "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10"
        ]

class SetupConfig:
    """Configuration for folder structure and session management."""
    def __init__(self, working_folder: str = "data", session_folder: str = "session", checkpoints_folder: str = "checkpoints", ground_truth_folder: str = "ground_truth"):
        self.working_folder = working_folder
        self.session_folder = session_folder
        self.checkpoints_folder = checkpoints_folder
        self.ground_truth_folder = ground_truth_folder

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

class FileMatchConfig:
    """Configuration for matching files between source and ground truth folders."""
    def __init__(self, source_folder: Path, ground_truth_folder: Path, output_folder: Path = None, recursive: bool = True):
        self.source_folder = source_folder
        self.ground_truth_folder = ground_truth_folder
        self.output_folder = output_folder
        self.recursive = recursive

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

class AzureDownloadConfig:
    """Configuration for Azure download flow."""
    def __init__(self, prefix: str = None, output_dir: str = "data/output"):
        self.prefix = prefix
        self.output_dir = output_dir

class CompareFilesConfig:
    """Configuration for comparing JSON labels with ground truth CSV files."""
    def __init__(self, matched_files: list[tuple[Path, Path]], output_folder: Path = None, report_folder: Path = None):
        self.matched_files = matched_files
        self.output_folder = output_folder
        self.report_folder = report_folder

    def get_output_path(self, json_path: Path) -> Path:
        """Get the output path for a corrected JSON file."""
        if self.output_folder is None:
            return json_path.parent / json_path.name.lower()
        return self.output_folder / json_path.name.lower()

    def get_report_path(self, json_path: Path) -> Path:
        """Get the output path for a correction report CSV."""
        if self.report_folder is None:
            return json_path.parent / f"{json_path.stem}.csv"
        return self.report_folder / f"{json_path.stem}.csv"

class UploadLabelsConfig:
    """Configuration for uploading label files to Azure."""
    def __init__(self, source_folder: Path, prefix: str = None, file_pattern: str = "*.labels.json"):
        self.source_folder = source_folder
        self.prefix = prefix
        self.file_pattern = file_pattern

    def get_source_files(self) -> list[Path]:
        """Get all matching files from source folder."""
        if not self.source_folder.exists():
            raise ValueError(f"Source directory does not exist: {self.source_folder}")
        return list(self.source_folder.glob(self.file_pattern))

class PipelineConfig:
    """High-level configuration that holds all other configs."""
    def __init__(self, setup: SetupConfig, azure_download: AzureDownloadConfig, file_match: FileMatchConfig = None, compare_files: CompareFilesConfig = None, upload_labels: UploadLabelsConfig = None):
        self.setup = setup
        self.azure_download = azure_download
        self.file_match = file_match
        self.compare_files = compare_files
        self.upload_labels = upload_labels

# Remove the default settings instance and get_settings function
# They will be created when needed in the flow 