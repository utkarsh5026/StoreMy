-- StoreMy Database - Quick Test
-- A minimal example to verify database functionality

-- Create a simple users table
CREATE TABLE users (
    id INT,
    username VARCHAR,
    email VARCHAR,
    age INT
);

-- Insert test data
INSERT INTO users VALUES (1, 'john_doe', 'john@example.com', 28);
INSERT INTO users VALUES (2, 'jane_smith', 'jane@example.com', 32);
INSERT INTO users VALUES (3, 'bob_jones', 'bob@example.com', 45);
INSERT INTO users VALUES (4, 'alice_wong', 'alice@example.com', 24);

-- View all users
SELECT * FROM users;

-- Filter users over 30
SELECT * FROM users WHERE users.age > 30;

-- Count total users
SELECT COUNT(users.id) FROM users;

-- Update a user's age
UPDATE users SET age = 29 WHERE users.id = 1;

-- Delete a user
DELETE FROM users WHERE users.id = 4;

-- Final view
SELECT * FROM users;
