import pytest
from pathlib import Path
import json
import os
import shutil

from src.flows.azure_download_flow import azure_download_flow

def test_azure_download_flow_real():
    """
    Test the Azure download flow with real Azure connections.
    This test requires valid Azure credentials and access to the storage account.
    """
    # Setup test output directory
    test_output_dir = "data/test_output"
    if os.path.exists(test_output_dir):
        shutil.rmtree(test_output_dir)
    
    # Run the flow with a test prefix
    result = azure_download_flow(
        prefix="test",  # Adjust this prefix based on your test data
        output_dir=test_output_dir
    )
    
    # Verify results
    assert isinstance(result, dict)
    assert "total_files" in result
    assert "local_paths" in result
    assert isinstance(result["local_paths"], list)
    
    # Check that output directory exists
    assert os.path.exists(test_output_dir)
    
    # Check that downloaded files exist and are valid JSON
    for file_path in result["local_paths"]:
        assert os.path.exists(file_path)
        with open(file_path) as f:
            try:
                json_data = json.load(f)
                assert isinstance(json_data, dict)
            except json.JSONDecodeError:
                pytest.fail(f"File {file_path} is not valid JSON")
    
    # Cleanup
    shutil.rmtree(test_output_dir)

def test_azure_download_flow_no_prefix():
    """
    Test the Azure download flow without a prefix.
    This will download all label files.
    """
    # Setup test output directory
    test_output_dir = "data/test_output_no_prefix"
    if os.path.exists(test_output_dir):
        shutil.rmtree(test_output_dir)
    
    # Run the flow without a prefix
    result = azure_download_flow(
        prefix=None,
        output_dir=test_output_dir
    )
    
    # Verify results
    assert isinstance(result, dict)
    assert "total_files" in result
    assert "local_paths" in result
    assert isinstance(result["local_paths"], list)
    
    # Check that output directory exists
    assert os.path.exists(test_output_dir)
    
    # Check that downloaded files exist and are valid JSON
    for file_path in result["local_paths"]:
        assert os.path.exists(file_path)
        with open(file_path) as f:
            try:
                json_data = json.load(f)
                assert isinstance(json_data, dict)
            except json.JSONDecodeError:
                pytest.fail(f"File {file_path} is not valid JSON")
    
    # Cleanup
    shutil.rmtree(test_output_dir) 