from prefect import flow
from pathlib import Path
from src.utils.config import SetupConfig

@flow(name="setup_flow")
def setup_flow(config: SetupConfig) -> dict:
    """
    Create the necessary folder structure for the pipeline.
    
    Args:
        config: SetupConfig containing folder structure parameters
        
    Returns:
        Dictionary with created folder paths
    """
    # Create folders
    config.create_folders()
    
    # Get paths
    session_path = config.get_session_path()
    checkpoints_path = config.get_checkpoints_path()
    ground_truth_path = config.get_ground_truth_path()
    
    return {
        "session_path": str(session_path),
        "checkpoints_path": str(checkpoints_path),
        "ground_truth_path": str(ground_truth_path)
    } 