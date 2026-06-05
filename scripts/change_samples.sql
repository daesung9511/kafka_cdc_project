UPDATE employees
SET city = 'Jersey City',
    salary = 95000
WHERE emp_id = 1;

DELETE FROM employees
WHERE emp_id = 2;

INSERT INTO employees (first_name, last_name, dob, city, salary)
VALUES ('Mike', 'Park', '1996-07-07', 'Seattle', 100000);

SELECT * FROM employees ORDER BY emp_id;
SELECT * FROM emp_cdc ORDER BY cdc_id;
