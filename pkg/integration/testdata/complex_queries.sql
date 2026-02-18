-- Complex query test cases
-- These queries test advanced SQL features

-- Aggregation with GROUP BY
SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department;

-- Join query
SELECT employees.name, departments.name, employees.salary
FROM employees
JOIN departments ON employees.department = departments.name;

-- Filtered join
SELECT employees.name, departments.budget
FROM employees
JOIN departments ON employees.department = departments.name
WHERE employees.salary > 85000;

-- Multiple aggregates
SELECT department, COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary)
FROM employees
GROUP BY department;

-- Count all employees
SELECT COUNT(*) FROM employees;

-- High salary employees
SELECT name, salary FROM employees WHERE salary > 90000;
