# -*- coding: utf-8 -*-
import time
from log_format import log_format
from handle_metrics import handleMetrics
from request_spark_rest_api import request_url


def main(app_url):
    hm = handleMetrics()
    logger.info("request url and obtain applications information")
    app_data = request_url(app_url)
    logger.info("write applications data to influxdb")
    app_ids = hm.write_applications_data(app_data)
    for app_id in app_ids:
        pre = root_url + "/" + app_id + "/"

        job_url = pre + "jobs"
        logger.info("request url and obtain jobs information")
        job_data = request_url(job_url)
        logger.info("write jobs data to influxdb")
        job_stages = hm.write_jobs_data(app_id, job_data)

        stage_url = pre + "stages"
        logger.info("request url and obtain stages information")
        stage_data = request_url(stage_url)
        logger.info("write stages data to influxdb")
        hm.write_stages_data(app_id, job_stages, stage_data)

        executor_url = pre + "executors"
        logger.info("request url and obtain executors information")
        executor_data = request_url(executor_url)
        logger.info("write executors data to influxdb")
        hm.write_executors_data(app_id, executor_data)

        rdd_url = pre + "storage/rdd"
        #rdd_data = request_url(rdd_url)
        #hm.write_rdds_data(rdd_data)

        streaming_statistic_url = pre + "streaming/statistics"
        #streaming_statistic_data = request_url(streaming_statistic_url)
        #hm.write_streaming__statistic_data(app_id, streaming_statistic_data)

        streaming_receiver_url = pre + "streaming/receivers"
        #streaming_receiver_data = request_url(streaming_receiver_url)
        #hm.write_streaming__receiver_data(app_id, streaming_receiver_data)

        streaming_batch_url = pre + "streaming/batches"
        #streaming_batch_data = request_url(streaming_batch_url)
        #hm.write_streaming__batch_data(app_id, streaming_batch_data)

        environment_url = pre + "environment"
        #environment_data = request_url(environment_url)
        #hm.write_environment_data(app_id, environment_data)


logger = log_format()
while True:
    try:
        port = 4040
        root_url = "http://h002194.mars.grid.sina.com.cn:" + str(port) + "/api/v1/applications"
        main(root_url)
        logger.info("the time interval is 10 seconds")
        time.sleep(10)
    except Exception as e:
        logger.error(e)
        break
