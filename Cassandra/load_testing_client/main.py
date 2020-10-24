import argparse
import asyncio
import json
import logging
import os
import random
from typing import Union

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def initialize_logger() -> logging.Logger:
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

	return root_logger


def initialize_argument_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser()
	parser.add_argument("-k", "--keyspace")
	parser.add_argument("-c", "--count", type = int, help = "count of rows to insert")
	parser.add_argument(
		"-p",
		"--parallel",
		type = int,
		help = "count of parallel connections",
		default = 1)
	parser.add_argument(
		"--min-id",
		type = int,
		help = "minimum allowed row id (default 0)",
		default = 0)
	parser.add_argument(
		"--max-id",
		type = int,
		help = "maximum allowed row id (default 2147483647)",
		default = 2147483647)
	parser.add_argument("table_name", metavar = "TABLE_NAME")
	return parser


def get_json_config(json_file: str):
	logging.info("Reading config from JSON file " + json_file)

	file = open(json_file, "r")
	return json.load(file)


async def start_test_on_new_connection(
		cluster: Cluster,
		keyspace_name: Union[str, None],
		table_name: str,
		rows_count: Union[int, None],
		min_row_id: int,
		max_row_id: int):
	session = cluster.connect()
	logging.info("Connection to Cassandra established")

	if keyspace_name is not None:
		session.set_keyspace(keyspace_name)
	query_to_prepare = f"INSERT INTO {table_name}(id, name) VALUES (?, ?)"
	logging.info("Preparing query:" + os.linesep + query_to_prepare)
	prepared_insert_query = session.prepare(query_to_prepare)

	logging.info(f"Inserting {rows_count} rows into table {table_name} with prepared query")

	def insert_new_row():
		row_id = int(round(random.uniform(min_row_id, max_row_id)))
		session.execute(prepared_insert_query, [row_id, f"'name_{row_id}'"])

	if rows_count is None:
		while True:
			insert_new_row()
	else:
		for _ in range(0, rows_count):
			insert_new_row()


def run_load_test(
		keyspace_name: Union[str, None],
		table_name: str,
		rows_count: Union[int, None],
		connections_count: int,
		min_row_id: int,
		max_row_id: int,
		config):
	username = config["username"]
	password = config["password"]
	hosts = config["hosts"]

	logging.info("Trying to connect to Cassandra")
	auth_provider = PlainTextAuthProvider(username = username, password = password)
	cluster = Cluster(hosts, auth_provider = auth_provider)

	event_loop = asyncio.get_event_loop()
	futures = [
		event_loop.create_task(start_test_on_new_connection(
			cluster,
			keyspace_name,
			table_name,
			rows_count,
			min_row_id,
			max_row_id))
		for _ in range(connections_count)]
	event_loop.run_until_complete(asyncio.wait(futures))

	logging.info(f"Successfully inserted into table {table_name}")


def main():
	initialize_logger()

	argument_parser = initialize_argument_parser()
	args = argument_parser.parse_args()

	config = get_json_config("config.json")

	run_load_test(
		args.keyspace,
		args.table_name,
		args.count,
		args.parallel,
		args.min_id,
		args.max_id,
		config)


if __name__ == "__main__":
	main()
