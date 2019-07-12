import boto3
import time
import datetime
import logging
import structlog
import os
import json
import sys
import requests
import re
from S3TextFromLambdaEvent import *

def lambda_handler(event, context):
	try:
		aws_request_id = ""
		aws_request_id = ""
		if context is not None:
			aws_request_id = context.aws_request_id

		print("Started")
		if "text_logging" in os.environ:
			log = structlog.get_logger()
		else:
			log = setup_logging("aws-download-url", event, aws_request_id)


		for records in event["Records"]:
			message = json.loads(records["Sns"]["Message"])
			print(message)
			url = message["line"]
			res = download_page(url)
			status_code = res.status_code
			print(str(res.status_code) + "-" + url)
			result = {"processing_type" : "async download urls", "url" : url, "status_code" : res.status_code, "length" : len(res.text)}
			log.critical("processed url", result=result)
			filename = re.sub(r"[^a-zA-Z0-9-_]", "_", url) + ".html"
			create_s3_text_file("svz-aws-download-webpages", "output/" + filename, res.text)
			print("Finished")

	except Exception as e:
		print("Exception: "+ str(e))
		raise(e)
		return {"msg" : "Exception"}

	return {"msg" : "Success"}




def setup_logging(lambda_name, lambda_event, aws_request_id):
	logging.basicConfig(
		format="%(message)s",
		stream=sys.stdout,
		level=logging.INFO
	)
	structlog.configure(
		processors=[
			structlog.stdlib.filter_by_level,
			structlog.stdlib.add_logger_name,
			structlog.stdlib.add_log_level,
			structlog.stdlib.PositionalArgumentsFormatter(),
			structlog.processors.TimeStamper(fmt="iso"),
			structlog.processors.StackInfoRenderer(),
			structlog.processors.format_exc_info,
			structlog.processors.UnicodeDecoder(),
			structlog.processors.JSONRenderer()
		],
		context_class=dict,
		logger_factory=structlog.stdlib.LoggerFactory(),
		wrapper_class=structlog.stdlib.BoundLogger,
		cache_logger_on_first_use=True,
	)
	log = structlog.get_logger()
	log = log.bind(aws_request_id=aws_request_id)
	log = log.bind(lambda_name=lambda_name)
	log.critical("started", input_events=json.dumps(lambda_event, indent=3))

	return log


def download_page(url):
	res = requests.get(url, allow_redirects=True, timeout=10)
	return res

