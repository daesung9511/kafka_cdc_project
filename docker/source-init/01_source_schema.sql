DROP TABLE IF EXISTS emp_cdc CASCADE;
DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS producer_offset CASCADE;

CREATE TABLE employees (
                           emp_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                           first_name VARCHAR(100),
                           last_name VARCHAR(100),
                           dob DATE,
                           city VARCHAR(100),
                           salary INT,
                           synced_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE emp_cdc (
    cdc_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY ,
    emp_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE producer_offset (
    producer_id INT PRIMARY KEY ,
    last_cdc_id INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE OR REPLACE FUNCTION trigger_cdc()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        IF tg_op = 'INSERT' THEN
            INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
            VALUES (new.emp_id, new.first_name, new.last_name, new.dob, new.city, new.salary, 'INSERT');
            RETURN NEW;
        ELSIF tg_op = 'UPDATE' THEN
            INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
            VALUES (new.emp_id, new.first_name, new.last_name, new.dob, new.city, new.salary, 'UPDATE');
            RETURN NEW;
        ELSIF tg_op = 'DELETE' THEN
            INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
            VALUES (old.emp_id, old.first_name, old.last_name, old.dob, old.city, old.salary, 'DELETE');
            RETURN OLD;
        end if;
        RETURN NULL;
    end;
    $$;

CREATE OR REPLACE TRIGGER trg_emp_cdc
    AFTER INSERT OR DELETE OR UPDATE ON employees
    FOR EACH ROW
    EXECUTE FUNCTION trigger_cdc();


INSERT INTO employees ( first_name, last_name, dob, city, salary)
VALUES ( 'Daniel', 'JIN', '1995-11-07', 'NJ', 71000);

DELETE FROM employees WHERE first_name='Daniel';
select * from emp_cdc;
select * from employees;
