# -*- coding: utf-8 -*-
import logging


def log_format():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(funcName)s %(levelname)s %(lineno)d %(message)s')
    logger = logging.getLogger(__name__)
    return logger
