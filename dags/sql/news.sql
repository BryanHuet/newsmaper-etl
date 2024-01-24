DROP TABLE news
CREATE TABLE news (
    id SERIAL PRIMARY KEY,
    country_id INT,
    source VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    link VARCHAR NOT NULL,
    origin VARCHAR NOT NULL,
    timestamp DATE NOT NULL,
    CONSTRAINT fk_countries
        FOREIGN KEY(country_id) 
            REFERENCES countries(id)
);