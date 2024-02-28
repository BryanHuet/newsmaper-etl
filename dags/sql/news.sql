CREATE TABLE IF NOT EXISTS news (
    id SERIAL PRIMARY KEY,

    id_country INT REFERENCES countries(id),
    id_source INT REFERENCES sources(id),
    id_date INT REFERENCES date(id),

    title VARCHAR NOT NULL,
    link VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    media VARCHAR
);