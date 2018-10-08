# -*- coding: utf-8 -*-
from influxdb import InfluxDBClient
from influxdb import exceptions
from log_format import log_format


def connect_client(points):
    host = '10.77.113.56'
    port = 8086
    user = 'root'
    password = 'kafka!@#'
    database = 'jmxDB'
    logger = log_format()
    try:
        # using Http
        client = InfluxDBClient(host, port, user, password, database, timeout=5)
        client.write_points(points)
        logger.info("write data succeeded")
    except exceptions.InfluxDBClientError as e:
        logger.error(e)
    finally:
        logger.info("close influxdb client")
        client.close()
