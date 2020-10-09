CREATE KEYSPACE test
	WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE test.test_table(
	id bigint,
	name text,
	PRIMARY KEY (id));
