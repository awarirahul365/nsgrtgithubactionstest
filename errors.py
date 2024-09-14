import requests
import logging
import traceback
import json
import os
import sys
from requests import HTTPError


def define_payload(
        app_name, 
        error_mess, 
        inn_exception=None, 
        trackback=None
    ):

    payload = {
        "Application": f"{app_name}",
        "Error": f"{error_mess}",
        "InnerException": f"{inn_exception}",
        "Trackback": f"{trackback}"
    }

    return payload


def send_payload_to_endpoint(
        payload, 
        endpoint: str,
        headers={"Content-Type": "application/json"}
    ) -> int:

    response = None
    if isinstance(payload, dict):
        payload = json.dumps(payload)
    try:
        response = requests.post(
            endpoint, 
            data=payload,                     
            headers=headers
        )
        response.raise_for_status
    except HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')
    except Exception as err:
        logging.error(f'Other error occurred: {err}')
   
    if response and hasattr(response, "status_code"):
        status = response.status_code
    else:
        status = 404

    return status


class MyException(Exception):

    def __init__(self, *args: object, **kwargs) -> None:
        super().__init__(*args)

        self.message = self.__str__()

        if "name" in kwargs:
            self.name = kwargs["name"]
        else:
            self.name = None

        self.send_error_email(**kwargs)

    def send_error_email(self, **kwargs):

        try:
            tb_str  = traceback.format_exc()
        except:
            tb_str = None
            logging.warning("There is no Traceback to fetch")

        try:
            inn_exc = sys.exc_info()[1]
            ie_str  = "{}: {}".format(inn_exc.exc_type, inn_exc.exc_msg)
        except:
            ie_str = None
            logging.warning("There is no InnerException to fetch")
        
        error_mess = self.message
        if self.name:
            error_mess = f"{self.name}: {error_mess}"

        app_name = os.getenv("ApplicationName")
        if not app_name or not len(app_name)>0:
            raise NameError("'ApplicationName' not was not defined in environment.")

        endpoint = os.getenv("ErrorEndpoint")
        if not endpoint or not len(endpoint)>0:
            raise NameError("'ErrorEnpoint' not was not defined in environment.")

        payload = define_payload(
            app_name, 
            error_mess,
            inn_exception=ie_str,
            trackback=tb_str
        )
        send_payload_to_endpoint(payload, endpoint)
        logging.error(json.dumps(payload, indent=2))