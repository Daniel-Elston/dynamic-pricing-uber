from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from pprint import pformat

import requests
from requests.auth import HTTPDigestAuth

import utils.my_globs as my_globs
from utils.view_file import FileHandler
# from pprint import pprint


def auth_manager():
    api_creds = {
        'USERNAME': os.getenv('USERNAME'),
        'PASSWORD': os.getenv('PASSWORD'),
    }
    api_config = {
        'BASE_URL': os.getenv('BASE_URL'),
    }
    return api_creds, api_config


@dataclass
class ConnState:
    api_creds: dict = field(init=False)
    api_config: dict = field(init=False)
    sleep_interval: int = 30

    def __post_init__(self):
        self.api_creds, self.api_config = auth_manager()
        post_init_dict = {
            'api_creds': self.api_creds,
            'api_config': self.api_config,
            'sleep_interval': self.sleep_interval,
        }
        logging.debug(f"Initialized DataState: {pformat(post_init_dict)}")


class RequestData:
    def __init__(self, conn_state: ConnState):
        self.file_handler = FileHandler()
        self.save_path = Path(my_globs.project_config['api']['static'])
        self.api_creds = conn_state.api_creds
        self.api_config = conn_state.api_config
        self.sleep_interval = conn_state.sleep_interval

    def make_request(self, endpoint, params=None):
        url = f"{self.api_config['BASE_URL']}/{endpoint}"
        logging.debug(f'FULL URL: {url}?{params}')

        try:
            logging.debug('Attempting request...')
            response = requests.get(
                url, params,
                auth=HTTPDigestAuth(self.api_creds['USERNAME'], self.api_creds['PASSWORD'])
            )
            if response.status_code == 200:
                logging.debug(f"SUCCESSFUL Request: {response.status_code}")
                return response.json()
            if response.status_code == 429:
                logging.error(
                    f"API limit reached ERROR: {response.status_code}, Sleeping for {self.sleep_interval} seconds...")
                time.sleep(int(self.sleep_interval))
                return None
            else:
                logging.error(f"ERROR: {response.status_code}, {response.text}")
                return None
        except requests.RequestException as e:
            logging.error(f"Request failed: {e}")
            return None

    def fetch_data(self, endpoint, params):
        try:
            response = self.make_request(endpoint, json.dumps(params) if params else None)
            # self.file_handler.save_json(response, Path(f'{self.save_path}/{filename}.json'))
        except Exception as e:
            logging.error(f"Error: {e}")
        return response
