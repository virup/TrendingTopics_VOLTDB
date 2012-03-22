-- word count table --
CREATE TABLE WORDCOUNT
(
	word 	VARCHAR(100) NOT NULL,
	wcount  INTEGER,
	time    BIGINT,
	PRIMARY KEY (word)
);

-- statistics ---
CREATE TABLE STATS
(
	operation varchar(10),
	opscount  INTEGER
);

-- insert values in STATS table --
INSERT INTO STATS VALUES ('read', 0);
INSERT INTO STATS VALUES ('write', 0);
INSERT INTO STATS VALUES ('update', 0);
