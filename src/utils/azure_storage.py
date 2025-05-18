import json
import os
from typing import List, Dict, Any

from azure.storage.blob import BlobServiceClient, ContainerClient
from src.utils.config import get_settings

class AzureStorageClient:
    """Client for interacting with Azure Blob Storage."""
    
    def __init__(self):
        settings = get_settings()
        self.connection_string = settings.azure_storage_connection_string
        self.source_container = settings.azure_source_container_name
        self.destination_container = settings.azure_destination_container_name
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        
    def get_source_container_client(self) -> ContainerClient:
        """Get a client for the source container."""
        return self.blob_service_client.get_container_client(self.source_container)
    
    def get_destination_container_client(self) -> ContainerClient:
        """Get a client for the destination container."""
        return self.blob_service_client.get_container_client(self.destination_container)
    
    def list_blobs(self, container_name: str, prefix: str = None) -> List[str]:
        """List all blobs in the specified container with optional prefix."""
        container_client = self.blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs(name_starts_with=prefix)
        return [blob.name for blob in blobs]
    
    def download_json_blob(self, container_name: str, blob_name: str) -> Dict[str, Any]:
        """Download and parse a JSON blob."""
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        # Download the blob content
        download_stream = blob_client.download_blob()
        content = download_stream.readall()
        
        # Parse JSON content
        return json.loads(content)
    
    def save_json_locally(self, data: Dict[str, Any], local_path: str) -> str:
        """Save JSON data to a local file."""
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        return local_path
    
    def upload_json_blob(self, container_name: str, blob_name: str, data: Dict[str, Any], overwrite: bool = True) -> None:
        """
        Upload JSON data as a blob.
        
        Args:
            container_name: Name of the container to upload to
            blob_name: Name of the blob to create/update
            data: JSON data to upload
            overwrite: Whether to overwrite existing blob (default: True)
        """
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        # Convert data to JSON string
        json_data = json.dumps(data, indent=2)
        
        # Upload JSON data
        blob_client.upload_blob(json_data, overwrite=overwrite) 