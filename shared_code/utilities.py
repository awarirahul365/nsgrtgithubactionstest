from typing import Any, Optional

import json
import asyncio
import requests
import logging
from io import BytesIO
import pandas as pd
from azure.core.exceptions import HttpResponseError

class Utilities(object):

    @staticmethod
    def chunks(lst: list, n: int):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    @staticmethod
    def flattened_function(mainlist):
        fl_list=[d for sublist in mainlist for d in sublist]
        return fl_list

    @staticmethod
    async def gather_with_concurrency(n: int, *tasks):
        """Limits tasks concurrency to n sized chunks"""
        semaphore = asyncio.Semaphore(n)

        async def sem_task(task):
            async with semaphore:
                return await task
        return await asyncio.gather(*(sem_task(task) for task in tasks))

    @staticmethod
    async def post_message(url: str, payload: Any):
        '''Post request'''
        if not url:
            logging.info('Notification url was not defined.')
            return None

        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        logging.info(f'Post alert returned {response.text}')
        return response
    
    @staticmethod
    def extract_string(full_string: str, start_string:str, end_string: Optional[str] = None):
        """Extracts the string between start_string and end_string"""

        name      = None
        end_index = -1
        start_index = full_string.find(start_string)
        if start_index > -1:
            start_index += len(start_string)
            if end_string is not None:
                end_index = full_string.find(end_string, start_index)
                if end_index > -1:
                    name = full_string[start_index:end_index]
            else:
                name = full_string[start_index:]
        
        else:
            logging.warning("We couldn't find the start string. Nothing will be returned.")

        return name

    def get_resource_value(resource_uri: str, resource_name: str):
        """Gets the resource name based on resource type
        Function that returns the name of a resource from resource id/uri based on
        resource type name.
        Args:
            resource_uri (string): resource id/uri
            resource_name (string): Name of the resource type, e.g. capacityPools
        Returns:
            string: Returns the resource name
        """

        if not resource_uri.strip():
            return None

        if not resource_name.startswith('/'):
            resource_name = '/{}'.format(resource_name)

        if not resource_uri.startswith('/'):
            resource_uri = '/{}'.format(resource_uri)

        # Checks to see if the ResourceName and ResourceGroup is the same name and
        # if so handles it specially.
        rg_resource_name = '/resourceGroups{}'.format(resource_name)
        rg_index = resource_uri.lower().find(rg_resource_name.lower())
        # dealing with case where resource name is the same as resource group
        if rg_index > -1:
            removed_same_rg_name = resource_uri.lower().split(
                resource_name.lower())[-1]
            return removed_same_rg_name.split('/')[1]

        index = resource_uri.lower().find(resource_name.lower())
        if index > -1:
            res = resource_uri[index + len(resource_name):].split('/')
            if len(res) > 1:
                return res[1]

        return None

    @staticmethod
    def get_bytes_in_tib(size):
        """Converts a value from bytes to TiB
        This function converts a value in bytes into TiB
        Args:
            size (long): Size in bytes
        Returns:
            int: Returns value in TiB
        """
        if size != None:
            new_size = size / 1024 / 1024 / 1024 / 1024
        else:
            new_size = None
        return new_size

    @staticmethod
    def get_bytes_in_gib(size):
        """Converts a value from bytes to GiB
        This function converts a value in bytes into GiB
        Args:
            size (long): Size in bytes
        Returns:
            int: Returns value in GiB
        """
        if size != None:
            new_size = size / 1024 / 1024 / 1024
        else:
            new_size = None
        return new_size

    @staticmethod
    def get_tib_in_bytes(size):
        """Converts a value from TiB to bytes
        This function converts a value in bytes into TiB
        Args:
            size (int): Size in TiB
        Returns:
            long: Returns value in bytes
        """
        if size != None:
            new_size = size * 1024 * 1024 * 1024 * 1024
        else:
            new_size = None
        return new_size

    @staticmethod
    def get_gib_in_bytes(size):
        """Converts a value from GiB to bytes
        This function converts a value in bytes into GiB
        Args:
            size (int): Size in GiB
        Returns:
            long: Returns value in bytes
        """
        if size is not None:
            new_size = size * 1024 * 1024 * 1024
        else:
            new_size = None
        return new_size

    @staticmethod
    def resource_exists(resource_client, resource_id, api_version):
        """Generic function to check for existing Azure function
        This function checks if a specific Azure resource exists based on its
        resource Id.
        Args:
            client (ResourceManagementClient): Azure Resource Manager Client
            resource_id (string): Resource Id of the resource to be checked upon
            api_version (string): Resource provider specific API version
        """

        try:
            return resource_client.resources.check_existence_by_id(resource_id, api_version)
        except HttpResponseError as e:
            if e.status_code == 405: # HEAD not supported
                try:
                    resource_client.resources.get_by_id(resource_id, api_version)
                    return True
                except HttpResponseError as he:
                    if he.status_code == 404:
                        return False
            raise # If not 405 or 404, not expected