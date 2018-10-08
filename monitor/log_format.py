# -*- coding: utf-8 -*-
import logging


def log_format():
    #log information is printed to the screen
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s %(funcName)s %(levelname)s %(lineno)d %(message)s')
    logger = logging.getLogger()
    return logger
