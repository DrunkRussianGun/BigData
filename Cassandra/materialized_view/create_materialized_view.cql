USE test;
CREATE MATERIALIZED VIEW employees_by_name AS
	SELECT * FROM employees
	WHERE dept_name IS NOT NULL AND name IS NOT NULL
	PRIMARY KEY (name, dept_name, id);
