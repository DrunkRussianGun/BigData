CREATE KEYSPACE IF NOT EXISTS test
	WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE test;

CREATE TABLE employees (
   id bigint,
   dept_name TEXT,
   name TEXT,
   PRIMARY KEY ((id), dept_name)
);
