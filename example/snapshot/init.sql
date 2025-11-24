-- Initialize database with sample data for snapshot example

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text,
    created_on timestamptz DEFAULT now()
);

-- Create books table
CREATE TABLE IF NOT EXISTS books (
    id serial PRIMARY KEY,
    title text NOT NULL,
    author text NOT NULL,
    isbn text,
    created_on timestamptz DEFAULT now()
);

-- Insert 1000 users
INSERT INTO users (name, email)
SELECT
    'User-' || i,
    'user' || i || '@example.com'
FROM generate_series(1, 1000) AS i;

-- Insert 500 books
INSERT INTO books (title, author, isbn)
SELECT
    'Book Title ' || i,
    'Author-' || (i % 50),
    'ISBN-' || LPAD(i::text, 10, '0')
FROM generate_series(1, 500) AS i;

-- Log the initialization
DO $$
BEGIN
    RAISE NOTICE 'Database initialized with % users and % books',
        (SELECT COUNT(*) FROM users),
        (SELECT COUNT(*) FROM books);
END $$;

