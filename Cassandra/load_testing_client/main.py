import argparse
import asyncio
import json
import logging
import os
import random
from concurrent.futures import ProcessPoolExecutor
from typing import Union

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import RoundRobinPolicy


def initialize_logger(log_prefix: str = "") -> logging.Logger:
	log_formatter = logging.Formatter(f"%(asctime)s [%(levelname)s] {log_prefix}%(message)s")
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
		help = "count of clients working in parallel",
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


def run_new_client(
		number: int,
		keyspace_name: Union[str, None],
		table_name: str,
		rows_count: Union[int, None],
		min_row_id: int,
		max_row_id: int):
	initialize_logger(f"Client {number}: ")

	config = get_json_config("config.json")
	username = config["username"]
	password = config["password"]
	hosts = config["hosts"]

	logging.info("Trying to connect to Cassandra")
	auth_provider = PlainTextAuthProvider(username = username, password = password)
	execution_profile = ExecutionProfile(load_balancing_policy = RoundRobinPolicy())
	cluster = Cluster(
		hosts,
		auth_provider = auth_provider,
		execution_profiles = {"default": execution_profile})
	session = cluster.connect(keyspace = keyspace_name)
	logging.warning(f"Connection established")

	query_to_prepare = f"INSERT INTO {table_name}(id, name) VALUES (?, ?)"
	logging.info("Preparing query:" + os.linesep + query_to_prepare)
	prepared_insert_query = session.prepare(query_to_prepare)

	rows_count_str = str(rows_count) if rows_count is not None else "infinite"
	logging.info(f"Inserting {rows_count_str} rows into table {table_name} with prepared query")

	rows_counts = {
		"inserted": 0,
		"failed": 0
	}

	# noinspection PyShadowingNames
	def insert_new_row():
		row_id = int(round(random.uniform(min_row_id, max_row_id)))
		# noinspection PyBroadException
		try:
			session.execute(prepared_insert_query, [row_id, f"'name_{row_id}'"])
		except Exception:
			rows_counts["failed"] += 1
			if rows_counts["failed"] % 1000 == 0:
				logging.warning(f"Failed to insert {rows_counts['failed']} rows")

		rows_counts["inserted"] += 1
		if rows_counts["inserted"] % 1000 == 0:
			logging.info(f"Inserted {rows_counts['inserted']} rows")

	if rows_count is None:
		while True:
			insert_new_row()
	else:
		for _ in range(0, rows_count):
			insert_new_row()


async def run_load_test_async(
		keyspace_name: Union[str, None],
		table_name: str,
		rows_count: Union[int, None],
		clients_count: int,
		min_row_id: int,
		max_row_id: int):
	loop = asyncio.get_running_loop()
	with ProcessPoolExecutor() as pool:
		tasks = [
			loop.run_in_executor(
				pool,
				run_new_client,
				number,
				keyspace_name,
				table_name,
				rows_count,
				min_row_id,
				max_row_id)
			for number in range(clients_count)]
		await asyncio.wait(tasks)

	logging.info(f"Successfully inserted into table {table_name}")


def main():
	initialize_logger()

	argument_parser = initialize_argument_parser()
	args = argument_parser.parse_args()

	asyncio.run(
		run_load_test_async(
			args.keyspace,
			args.table_name,
			args.count,
			args.parallel,
			args.min_id,
			args.max_id))


if __name__ == "__main__":
	main()
