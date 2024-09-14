import logging
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob.aio import ContainerClient
from azure.core.exceptions import ResourceNotFoundError
class BlobService:

    def __init__(self,storageaccount_endpoint,conn_str) -> None:
        self.storageaccounturl=storageaccount_endpoint
        self.conn_str=conn_str
        self._blobserviceclient=BlobServiceClient.from_connection_string(
            conn_str=conn_str
        )
        self._container_client=None

    async def read_container_file(self,blob_name,container_name):
        try:
            self._container_client=ContainerClient.from_connection_string(
                conn_str=self.conn_str,
                container_name=container_name
            )
            _blobclient=self._container_client.get_blob_client(
                blob=blob_name
            )
            blob_data=await _blobclient.download_blob()
            blob_text=await blob_data.readall()
            logging.info(f"Check blob details")
            return blob_text
        except Exception as e:
            logging.error(f"Failed to fetch {e}")
            return None    
        finally:
            if self._blobserviceclient:
                await self._blobserviceclient.close()
            if self._container_client:
                await self._container_client.close()

    async def check_container_exists(self,container_name):
        try:
            self._container_client=ContainerClient.from_connection_string(
                conn_str=self.conn_str,
                container_name=container_name
            )
            await self._container_client.get_container_properties()
            logging.info("Container Is present")
        except ResourceNotFoundError:
            logging.error(f"Creating container")
            await self._container_client.create_container()
            logging.info("Created Container")
        finally:
            if self._blobserviceclient:
                await self._blobserviceclient.close()
            if self._container_client:
                await self._container_client.close()

    async def upload_blob_to_container(self,data,file_name:str,container_name):
        try:
            await self.check_container_exists(container_name)
            _blobclient=self._blobserviceclient.get_blob_client(
                container=container_name,
                blob=file_name
            )
            await _blobclient.upload_blob(
                    data=data,
                    overwrite=True 
                )
            logging.info("Blob File Uploaded")
            return "File Uploaded check the storage account"
        except Exception as e:
            logging.error(f"Failed to push blob {e}")
            return None
        finally:
            if self._blobserviceclient:
                await self._blobserviceclient.close()
            if self._container_client:
                await self._container_client.close()

    async def close_clients(self):
        if self._blobserviceclient:
            await self._blobserviceclient.close()
        if self._container_client:
            await self._container_client.close()
            



        



    