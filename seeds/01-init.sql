-- Create the users table
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE
);

-- Crucial for CDC performance: Index the updated_at column
CREATE INDEX IF NOT EXISTS idx_users_updated_at ON users(updated_at);

-- Change this table definition:
CREATE TABLE IF NOT EXISTS watermarks (
    id BIGSERIAL PRIMARY KEY, -- Changed from SERIAL to BIGSERIAL
    consumer_id VARCHAR(255) NOT NULL UNIQUE,
    last_exported_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Idempotent Seeding: Only insert if the table is empty
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM users LIMIT 1) THEN
        INSERT INTO users (name, email, created_at, updated_at, is_deleted)
        SELECT
            'User ' || i,
            'user' || i || '_' || extract(epoch from now()) || '@example.com',
            -- created_at: Random timestamp within the last 30 days
            NOW() - (random() * interval '30 days'),
            -- updated_at: Will temporarily set to now, and fix in the next step
            NOW(),
            -- is_deleted: ~1% of records will be marked true
            random() < 0.01
        FROM generate_series(1, 100000) AS i;

        -- Ensure updated_at is logically AFTER created_at
        UPDATE users 
        SET updated_at = created_at + (random() * (NOW() - created_at));
    END IF;
END $$;