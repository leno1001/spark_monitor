# -*- coding: utf-8 -*-
import requests
from log_format import log_format


def request_url(url):
    logger = log_format()
    try:
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()
        else:
            logger.info(r.text)
    except Exception as e:
        logger.error(e)
