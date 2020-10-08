import json
import sys

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def get_json_config(json_file: str):
	file = open(json_file, "r")
	return json.load(file)


def run_load_test(keyspace_name, table_name, rows_count, config):
	username = config["username"]
	password = config["password"]
	hosts = config["hosts"]

	auth_provider = PlainTextAuthProvider(username = username, password = password)
	cluster = Cluster(hosts, auth_provider = auth_provider)
	session = cluster.connect()

	session.set_keyspace(keyspace_name)
	prepared_insert_query = session.prepare(f"INSERT INTO {table_name}(id, name) VALUES (?, ?)")
	for i in range(0, rows_count):
		session.execute(prepared_insert_query, [i, f"'name_{i}'"])


def main():
	if len(sys.argv) != 4:
		print(
			f"Usage: {sys.argv[0]} <keyspace name> <table name> <count of rows to insert>",
			file = sys.stderr)
		exit(1)

	keyspace_name = sys.argv[1]
	table_name = sys.argv[2]
	rows_count = int(sys.argv[3])

	config = get_json_config("config.json")

	run_load_test(keyspace_name, table_name, rows_count, config)


if __name__ == "__main__":
	main()
