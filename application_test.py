# -*- coding: utf-8 -*-
import requests
from influxdb import InfluxDBClient
from influxdb import exceptions
import logging
import time
import datetime


class SparkMonitor(object):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(funcName)s %(levelname)s %(lineno)d %(message)s')
    logger = logging.getLogger(__name__)

    def __init__(self):
        super(SparkMonitor, self).__init__()

    @staticmethod
    def connect_client(points):
        host = '10.77.113.56'
        port = 8086
        user = 'root'
        password = 'kafka!@#'
        database = 'jmxDB'
        try:
            client = InfluxDBClient(host, port, user, password, database)
            client.write_points(points)
            SparkMonitor.logger.info("write data succeeded")
        except exceptions.InfluxDBClientError as e:
            SparkMonitor.logger.error(e)
        finally:
            SparkMonitor.logger.info("close influxdb client")
            client.close()

    @staticmethod
    def request_url(url):
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return r.json()
            else:
                SparkMonitor.logger.info(r.text)
        except Exception as e:
            SparkMonitor.logger.error(e)

    def write_applications_data(self, *applications):
        app_ids = []
        try:
            for application in applications[0]:
                info = application.get("attempts")[0]
                status_flag = info.get("completed")
                if status_flag:
                    status = "completed"
                    endTime = info.get("endTime")
                else:
                    status = "running"
                    endTime = "unknown"
                user = info.get("sparkUser")
                if not user:
                    user = "unknown"
                applications_points = [{
                    "measurement": "sparkMonitorRestApiApplicationsLevel",
                    "tags": {
                        "id": application.get("id")
                    },
                    "fields": {
                        "name": application.get("name"),
                        "startTime": info.get("startTime"),
                        "endTime": endTime,
                        "sparkUser": user,
                        "status": status
                    }
                }]
                app_ids.append(application.get("id"))
                self.connect_client(applications_points)
            return tuple(app_ids)
        except Exception as e:
            SparkMonitor.logger.error(e)

    def write_jobs_data(self, appId, *jobs):
        job_stages = {}
        try:
            for job in jobs[0]:
                GMT_FORMAT = '%Y-%m-%dT%H:%M:%S.%fGMT'
                st = job.get("submissionTime")
                sti = datetime.datetime.strptime(st, GMT_FORMAT)
                startTime = time.mktime(sti.timetuple())
                en = job.get("completionTime")
                end = datetime.datetime.strptime(en, GMT_FORMAT)
                endTime = time.mktime(end.timetuple())
                totalDuration = endTime - startTime
                jobs_points = [{
                    "measurement": "sparkMonitorRestApiJobsLevel",
                    "tags": {
                        "applicationId": appId,
                        "jobId": job.get("jobId"),

                    },
                    "fields": {
                        "name": job.get("name"),
                        "submissionTime": job.get("submissionTime"),
                        "completionTime": job.get("completionTime"),
                        "totalDuration": totalDuration,
                        "status": job.get("status"),
                        "numTasks": job.get("numTasks"),
                        "numActiveTasks": job.get("numActiveTasks"),
                        "numCompletedTasks": job.get("numCompletedTasks"),
                        "numSkippedTasks": job.get("numSkippedTasks"),
                        "numFailedTasks": job.get("numFailedTasks"),
                        "numActiveStages": job.get("numActiveStages"),
                        "numCompletedStages": job.get("numCompletedStages"),
                        "numSkippedStages": job.get("numSkippedStages"),
                        "numFailedStages": job.get("numFailedStages")
                    }
                }]
                job_stages.update({job.get("jobId"): job.get("stageIds")})
                self.connect_client(jobs_points)
            return job_stages
        except Exception as e:
            SparkMonitor.logger.error(e)

    def write_stages_data(self, appId, job_info, *stages):

        try:
            for stage in stages[0]:
                flag = False
                for k, v in job_info.iteritems():
                    for s in v:
                        if str(s) == str(stage.get("stageId")):
                            jobId = k
                            stages_points = [{
                                "measurement": "sparkMonitorRestApiStagesLevel",
                                "tags": {
                                    "applicationId": appId,
                                    "jobId": jobId,
                                    "stageId": stage.get("stageId"),
                                    "attemptId": stage.get("attemptId")
                                },
                                "fields": {
                                    "status": stage.get("status"),
                                    "numActiveTasks": stage.get("numActiveTasks"),
                                    "numCompletedTasks": stage.get("numCompletedTasks"),
                                    "numFailedTasks": stage.get("numFailedTasks"),
                                    "executorRuntime": stage.get("executorRuntime"),
                                    "inputBytes": stage.get("inputBytes"),
                                    "inputRecords": stage.get("inputRecords"),
                                    "outputBytes": stage.get("outputBytes"),
                                    "outputRecords": stage.get("outputRecords"),
                                    "shuffleReadBytes": stage.get("shuffleReadBytes"),
                                    "shuffleReadRecords": stage.get("shuffleReadRecords"),
                                    "shuffleWriteBytes": stage.get("shuffleWriteBytes"),
                                    "shuffleWriteRecords": stage.get("shuffleWriteRecords"),
                                    "memoryBytesSpilled": stage.get("memoryBytesSpilled"),
                                    "diskBytesSpilled": stage.get("diskBytesSpilled"),
                                    "name": stage.get("name"),
                                    "details": stage.get("details"),
                                    "schedulingPool": stage.get("schedulingPool"),
                                    "accumulatorUpdates": ','.join(stage.get("accumulatorUpdates"))
                                }
                            }]
                            flag = True
                            self.connect_client(stages_points)
                            break
                    if flag:
                        break
        except Exception as e:
            SparkMonitor.logger.error(e)

    def write_executors_data(self, app_id, *executors):
        try:
            for executor in executors[0]:
                executors_points = [{
                    "measurement": "sparkMonitorRestApiExecutorsLevel",
                    "tags": {
                        "applicationId": app_id,
                        "executorId": executor.get("id"),
                        "hostName": executor.get("hostPort").split(":")[0]
                    },
                    "fields": {
                        "rddBlocks": executor.get("rddBlocks"),
                        "memoryUsed": executor.get("memoryUsed"),
                        "diskUsed": executor.get("diskUsed"),
                        "activeTasks": executor.get("activeTasks"),
                        "failedTasks": executor.get("failedTasks"),
                        "completedTasks": executor.get("completedTasks"),
                        "totalTasks": executor.get("totalTasks"),
                        "totalDuration": executor.get("totalDuration"),
                        "totalInputBytes": executor.get("totalInputBytes"),
                        "totalShuffleRead": executor.get("totalShuffleRead"),
                        "totalShuffleWrite": executor.get("totalShuffleWrite"),
                        "maxMemory": executor.get("maxMemory"),
                        "stdout": executor.get("executorLogs").get("stdout"),
                        "stderr": executor.get("executorLogs").get("stderr")
                    }
                }]
                self.connect_client(executors_points)
        except Exception as e:
                SparkMonitor.logger.error(e)

    def write_rdds_data(self, *rdds):
        pass


def main(app_url):
    sm = SparkMonitor()
    app_data = SparkMonitor.request_url(app_url)
    app_ids = sm.write_applications_data(app_data)
    for app_id in app_ids:
        pre = root_url + "/" + app_id + "/"

        job_url = pre + "jobs"
        job_data = SparkMonitor.request_url(job_url)
        job_stages = sm.write_jobs_data(app_id, job_data)

        stage_url = pre + "stages"
        stage_data = SparkMonitor.request_url(stage_url)
        sm.write_stages_data(app_id, job_stages, stage_data)

        executor_url = pre + "executors"
        executor_data = SparkMonitor.request_url(executor_url)
        sm.write_executors_data(app_id, executor_data)

        rdd_url = pre + "storage/rdd"
        #rdd_data = SparkMonitor.request_url(rdd_url)
        #sm.write_rdds_data(rdd_data)


if __name__ == "__main__":
    root_url = "http://h002194.mars.grid.sina.com.cn:4040/api/v1/applications"
    #while True:
    main(root_url)
    #time.sleep(1)
