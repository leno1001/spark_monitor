# -*- coding: utf-8 -*-
import time
import datetime
from log_format import log_format
from connect_influxdb_client import connect_client


class handleMetrics(object):
    logger = log_format()

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
                        "applicationId": application.get("id")
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
                connect_client(applications_points)
            return tuple(app_ids)
        except Exception as e:
            handleMetrics.logger.error(e)

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
                        "numStages": len(job.get("stageIds")),
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
                connect_client(jobs_points)
            return job_stages
        except Exception as e:
            handleMetrics.logger.error(e)

    def write_stages_data(self, appId, job_info, *stages):

        try:
            for stage in stages[0]:
                flag = False
                for k, v in job_info.iteritems():
                    for s in v:
                        if str(s) == str(stage.get("stageId")):
                            jobId = k
                            nat = stage.get("numActiveTasks")
                            nct = stage.get("numCompleteTasks")
                            nft = stage.get("numFailedTasks")
                            totalTasks = nat + nct + nft
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
                                    "numActiveTasks": nat,
                                    "numCompletedTasks": nct,
                                    "numFailedTasks": nft,
                                    "totalTasks": totalTasks,
                                    "executorRuntime": stage.get("executorRunTime"),
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
                                    "schedulingPool": stage.get("schedulingPool", "unknown"),
                                    "accumulatorUpdates": len(stage.get("accumulatorUpdates"))
                                }
                            }]
                            flag = True
                            connect_client(stages_points)
                            break
                    if flag:
                        break
        except Exception as e:
            handleMetrics.logger.error(e)

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
                connect_client(executors_points)
        except Exception as e:
            handleMetrics.logger.error(e)

    def write_rdds_data(self, *rdds):
        pass

    def write_streaming__statistic_data(self, *statistics):
        pass

    def write_streaming__receiver_data(self, *receivers):
        pass

    def write_streaming__batch_data(self, *batches):
        pass

    def write_environment_data(self, *env):
        pass
