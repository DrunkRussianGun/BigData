import argparse
import json
import logging
import multiprocessing
import os
import random
from multiprocessing.context import Process
from time import sleep
from typing import Dict, List, Optional

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy


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
		"--min-int",
		type = int,
		help = "minimum allowed int value in rows (default 0)",
		default = 0)
	parser.add_argument(
		"--max-int",
		type = int,
		help = "maximum allowed int value in rows (default 2147483647)",
		default = 2147483647)
	parser.add_argument("table_name", metavar = "TABLE_NAME")
	return parser


def get_json(title: str, json_file: str):
	logging.info(f"Reading {title} from JSON file {json_file}")

	file = open(json_file, "r")
	return json.load(file)


def get_table_structure_key(keyspace_name: Optional[str], table_name: str) -> str:
	return f"{keyspace_name}.{table_name}" if keyspace_name is not None else table_name


def get_insert_query(table_name: str, table_structure) -> str:
	columns = ", ".join(column["name"] for column in table_structure)
	parameters = ", ".join("?" for _ in range(len(table_structure)))
	return f"INSERT INTO {table_name}({columns}) VALUES ({parameters})"


def generate_row_values(table_structure, min_int_value: int, max_int_value: int) -> List[str]:
	values = []
	for column in table_structure:
		int_value = int(round(random.uniform(min_int_value, max_int_value)))
		if column["type"] == "int":
			value = int_value
		elif column["type"] == "string":
			value = f"{column['name']}_{int_value}"
		else:
			raise ValueError(f"Unsupported type {column['type']} of column {column['name']}")
		values.append(value)
	return values


def run_load_test(
		number: int,
		keyspace_name: Optional[str],
		table_name: str,
		rows_count: Optional[int],
		min_int_value: int,
		max_int_value: int,
		shared_rows_counts: Dict[str, int]):
	tables_file_name = "tables.json"
	tables = dict(
		(get_table_structure_key(structure["keyspace"], structure["table"]), structure["structure"])
		for structure in get_json("table structures", tables_file_name))
	table_structure_key = get_table_structure_key(keyspace_name, table_name)
	if table_structure_key not in tables:
		raise RuntimeError(f"Structure of table {table_structure_key} not found in file {tables_file_name}")
	table_structure = tables[table_structure_key]

	config = get_json("config", "config.json")
	username = config["username"]
	password = config["password"]
	hosts = config["hosts"]

	logging.info("Trying to connect to Cassandra")
	auth_provider = PlainTextAuthProvider(username = username, password = password)
	execution_profile = ExecutionProfile(load_balancing_policy = WhiteListRoundRobinPolicy(hosts))
	cluster = Cluster(
		hosts,
		auth_provider = auth_provider,
		execution_profiles = {"default": execution_profile})
	session = cluster.connect(keyspace = keyspace_name)
	logging.warning(f"Connection established")

	query_to_prepare = get_insert_query(table_name, table_structure)
	logging.info("Preparing query:" + os.linesep + query_to_prepare)
	prepared_insert_query = session.prepare(query_to_prepare)

	rows_count_str = str(rows_count) if rows_count is not None else "infinite"
	logging.info(f"Inserting {rows_count_str} rows into table {table_name} with prepared query")

	inserted_rows_count_key = f"inserted_{number}"
	failed_rows_count_key = f"failed_{number}"
	rows_counts = {
		inserted_rows_count_key: 0,
		failed_rows_count_key: 0
	}

	# noinspection PyShadowingNames
	def insert_new_row():
		row_values = generate_row_values(table_structure, min_int_value, max_int_value)
		# noinspection PyBroadException
		try:
			session.execute(prepared_insert_query, row_values)
		except Exception:
			rows_counts[failed_rows_count_key] += 1
			if rows_counts[failed_rows_count_key] % 10 == 0:
				shared_rows_counts[failed_rows_count_key] = rows_counts[failed_rows_count_key]

		rows_counts[inserted_rows_count_key] += 1
		if rows_counts[inserted_rows_count_key] % 10 == 0:
			shared_rows_counts[inserted_rows_count_key] = rows_counts[inserted_rows_count_key]

	if rows_count is None:
		while True:
			insert_new_row()
	else:
		for _ in range(0, rows_count):
			insert_new_row()

	logging.warning(f"Finished")


def run_load_test_clients(clients_count: int):
	shared_rows_counts = multiprocessing.Manager().dict()
	last_failed_rows_count = [0]

	def log_rows_counts():
		current_rows_counts = dict(shared_rows_counts)
		inserted_rows_count = sum(
			map(
				lambda x: x[1],
				filter(
					lambda x: x[0].startswith("inserted"),
					current_rows_counts.items())))
		failed_rows_count = sum(
			map(
				lambda x: x[1],
				filter(
					lambda x: x[0].startswith("failed"),
					current_rows_counts.items())))

		log_message = f"Inserted {inserted_rows_count} rows, failed to insert {failed_rows_count} rows"
		if failed_rows_count % 1000 > last_failed_rows_count[0]:
			logging.warning(log_message)
			last_failed_rows_count[0] = failed_rows_count
		else:
			logging.info(log_message)

	clients = []
	for client_number in range(clients_count):
		client = Process(target = main, args = (True, client_number, shared_rows_counts))
		client.start()
		clients.append(client)

	alive_clients = clients
	while len(alive_clients) > 0:
		sleep(3)
		log_rows_counts()
		alive_clients = list(filter(lambda client: client.is_alive(), clients))


def main(
		child_process: bool = False,
		client_number: int = None,
		shared_rows_counts: Dict[str, int] = None):
	log_prefix = f"Client {client_number}: " if child_process else ""
	initialize_logger(log_prefix)

	argument_parser = initialize_argument_parser()
	args = argument_parser.parse_args()

	if child_process:
		run_load_test(
			client_number,
			args.keyspace,
			args.table_name,
			args.count,
			args.min_int,
			args.max_int,
			shared_rows_counts)
	else:
		run_load_test_clients(args.parallel)


if __name__ == "__main__":
	multiprocessing.set_start_method("spawn")
	main()
