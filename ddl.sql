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
