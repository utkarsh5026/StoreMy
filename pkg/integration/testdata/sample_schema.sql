-- Sample schema for integration testing
-- This file tests SQL file import functionality

-- Create employees table
CREATE TABLE employees (
    id INT PRIMARY KEY,
    name STRING,
    department STRING,
    salary INT,
    hire_date STRING
);

-- Create departments table
CREATE TABLE departments (
    id INT PRIMARY KEY,
    name STRING,
    budget INT
);

-- Insert sample employees
INSERT INTO employees VALUES (1, 'Alice Johnson', 'Engineering', 95000, '2023-01-15');
INSERT INTO employees VALUES (2, 'Bob Smith', 'Sales', 85000, '2023-02-20');
INSERT INTO employees VALUES (3, 'Charlie Brown', 'Engineering', 90000, '2023-03-10');
INSERT INTO employees VALUES (4, 'Diana Prince', 'HR', 80000, '2023-04-05');
INSERT INTO employees VALUES (5, 'Eve Wilson', 'Sales', 88000, '2023-05-12');

-- Insert departments
INSERT INTO departments VALUES (1, 'Engineering', 500000);
INSERT INTO departments VALUES (2, 'Sales', 300000);
INSERT INTO departments VALUES (3, 'HR', 200000);
INSERT INTO departments VALUES (4, 'Marketing', 250000);
