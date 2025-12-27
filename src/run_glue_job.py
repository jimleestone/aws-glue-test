import json
import os
from os.path import join

import boto3

glue = boto3.client("glue")


def handler(event, context):
    try:

        # Start the Glue job run
        job_name = os.getenv("GLUE_JOB_NAME")
        start_job_run_resp = glue.start_job_run(JobName=job_name, Arguments={})
        print("start_job_run_resp:", start_job_run_resp)

        # Report the successful job execution to CodePipeline
        print(f"submitting successful. result: ${start_job_run_resp}")
    except Exception as e:
        # Report the failed job execution to CodePipeline
        print("submitting unsuccessful job: " + str(e))
