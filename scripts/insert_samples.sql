INSERT INTO employees (first_name, last_name, dob, city, salary)
VALUES
('Daniel', 'Jin', '1998-01-15', 'New York', 90000),
('John', 'Kim', '1997-03-20', 'New Jersey', 85000),
('Sarah', 'Lee', '1999-11-05', 'Boston', 92000);

SELECT * FROM employees ORDER BY emp_id;
SELECT * FROM emp_cdc ORDER BY cdc_id;
