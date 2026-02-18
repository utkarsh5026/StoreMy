-- This is a single line comment
-- It should be ignored by the parser

CREATE TABLE commented_table (
    id INT PRIMARY KEY,
    value STRING
);

-- Another comment before INSERT
INSERT INTO commented_table VALUES (1, 'first');

-- One more comment
INSERT INTO commented_table VALUES (2, 'second');

-- Final comment at end of file
