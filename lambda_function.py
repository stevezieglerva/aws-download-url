import boto3
import time
import datetime
import logging
import structlog
import os
import json
import sys

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
			log = setup_logging("!Update me with Lambda name!", event, aws_request_id)

		print("Started")


			# CSV format
			quoted_filename = "\"" + dest_file + "\""
			quoted_text = "\"" + text + "\""
			quoted_project =  "\"" + project + "\""
			quoted_raw_filename =  "\"" + raw_filename + "\""
			quoted_file_ext =  "\"" + file_ext + "\""
			csv_line = "{}, {}, {}, {}, {}\n".format(quoted_filename, quoted_text, quoted_project, quoted_raw_filename, quoted_file_ext)
			response = stream_firehose_string("code-index-files-csv", csv_line)

			# Elasticsearch bulk format
			index_header = "{\"index\": {\"_index\": \"code-index\", \"_type\": \"doc\"}}"
			index_data = {"filename" : dest_file, "file_text" : text, "raw_filename" : raw_filename, "file_extension" : file_ext, "project" : project}
			index_data = add_timestamps_to_event(index_data)
			response = stream_firehose_string("code-index-files-es-bulk", index_header + "\n" + json.dumps(index_data) + "\n")




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
