CREATE TABLE IF NOT EXISTS date (
    id SERIAL PRIMARY KEY,
    hours INT NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL
);