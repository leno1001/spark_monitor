# -*- coding: utf-8 -*-
import requests
from log_format import log_format
from requests.exceptions import RequestException


def request_url(url):
    logger = log_format()
    try:
        '''
        application: completed,running;
        job: running,succeeded,failed,unknown;
        stage: active,complete,pending,failed
        '''
        status = "running"
        payload = {"status": status}
        #r = requests.get(url, params=payload, timeout=5)
        with requests.Session() as s:
            r = s.get(url, timeout=5)
            if r.status_code == 200:
                logger.info("request url succeeded and return data")
                return r.json()
            else:
                logger.info(r.text)
    except RequestException as e:
        logger.error(e)
