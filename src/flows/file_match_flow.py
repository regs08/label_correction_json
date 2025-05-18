from prefect import flow
from pathlib import Path
from typing import Dict, List, Tuple
from src.utils.config import FileMatchConfig
from src.tasks.file_processing_tasks import match_files, copy_to_checkpoint
from prefect import get_run_logger

@flow(name="file_match_flow")
def file_match_flow(config: FileMatchConfig) -> Dict[str, List[Tuple[str, str]]]:
    """
    Flow to match files between source and ground truth folders.
    
    Args:
        config: FileMatchConfig containing source and ground truth folder paths
        
    Returns:
        Dictionary containing:
        - matched_files: List of tuples (source_path, ground_truth_path)
    """
    logger = get_run_logger()
    logger.info("Starting file match flow")
    
    # Convert Path objects to strings for the match_files task
    source_dir = str(config.source_folder)
    ground_truth_dir = str(config.ground_truth_folder)
    logger.info(f"Source directory: {source_dir}")
    logger.info(f"Ground truth directory: {ground_truth_dir}")
    
    # Ensure directories exist
    if not Path(source_dir).exists():
        logger.error(f"Source directory does not exist: {source_dir}")
        return {"matched_files": []}
    if not Path(ground_truth_dir).exists():
        logger.error(f"Ground truth directory does not exist: {ground_truth_dir}")
        return {"matched_files": []}
    
    # Match files between source and ground truth
    logger.info("Calling match_files task...")
    matches = match_files(source_dir=source_dir, target_dir=ground_truth_dir)
    logger.info(f"Found {len(matches)} matches")
    
    # Convert matches to list of tuples
    logger.info("Converting matches to path tuples...")
    matched_pairs = []
    for source_file, ground_truth_file in matches.items():
        source_path = str(Path(source_dir) / source_file)
        ground_truth_path = str(Path(ground_truth_dir) / ground_truth_file)
        matched_pairs.append((source_path, ground_truth_path))
        logger.info(f"Matched: {source_path} -> {ground_truth_path}")
    
    logger.info(f"Returning {len(matched_pairs)} matched pairs")
    return {
        "matched_files": matched_pairs
    } 