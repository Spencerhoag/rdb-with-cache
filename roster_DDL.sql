CREATE DATABASE nfl_roster;


CREATE TABLE roster (
    name TEXT,
    number INT,
    pos TEXT,
    ht TEXT,
    wt INT,
    age INT,
    exp TEXT,
    college TEXT
);


COPY roster(name, number, pos, ht, wt, age, exp, college)
FROM '/Users/spencerhoag/Documents/GitHub/rdb-with-cache/full_nfl_roster.csv'
WITH (FORMAT csv, HEADER true);
