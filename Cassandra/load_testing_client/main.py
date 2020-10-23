import json
import logging
import os
import sys

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def configure_logging():
	log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
	root_logger = logging.getLogger()
	root_logger.setLevel(logging.DEBUG)

	log_directory_name = "logs"
	import os
	os.makedirs(log_directory_name, exist_ok=True)
	from datetime import datetime
	log_file_name = log_directory_name + "/log_" + datetime.today().strftime("%Y-%m-%d")
	os.close(os.open(log_file_name, os.O_CREAT))

	file_handler = logging.FileHandler(log_file_name)
	file_handler.setFormatter(log_formatter)
	file_handler.setLevel(logging.INFO)
	root_logger.addHandler(file_handler)

	console_handler = logging.StreamHandler()
	console_handler.setFormatter(log_formatter)
	console_handler.setLevel(logging.WARNING)
	root_logger.addHandler(console_handler)


def get_json_config(json_file: str):
	logging.info("Reading config from JSON file " + json_file)

	file = open(json_file, "r")
	return json.load(file)


def run_load_test(keyspace_name, table_name, rows_count, config):
	username = config["username"]
	password = config["password"]
	hosts = config["hosts"]

	logging.info("Trying to connect to Cassandra")
	auth_provider = PlainTextAuthProvider(username = username, password = password)
	cluster = Cluster(hosts, auth_provider = auth_provider)
	session = cluster.connect()
	logging.info("Connection to Cassandra established")

	session.set_keyspace(keyspace_name)
	query_to_prepare = f"INSERT INTO {table_name}(id, name) VALUES (?, ?)"
	logging.info("Preparing query:" + os.linesep + query_to_prepare)
	prepared_insert_query = session.prepare(query_to_prepare)

	logging.info(f"Inserting {rows_count} rows into table {table_name} with prepared query")
	for i in range(0, rows_count):
		session.execute(prepared_insert_query, [i, f"'name_{i}'"])
	logging.info(f"Successfully inserted into table {table_name}")


def main():
	configure_logging()

	if len(sys.argv) != 4:
		logging.error(
			f"Wrong command line arguments: {sys.argv}" + os.linesep
			+ "Expected: <path to application> <keyspace name> <table name> <count of rows to insert>")
		exit(1)

	keyspace_name = sys.argv[1]
	table_name = sys.argv[2]
	rows_count = int(sys.argv[3])

	config = get_json_config("config.json")

	run_load_test(keyspace_name, table_name, rows_count, config)


if __name__ == "__main__":
	main()
