CREATE TABLE title_basics
	(title_id char (9) PRIMARY KEY,
	title_type varchar (20),
	primary_title varchar (300),
	original_title varchar (300),
	is_adult boolean,
	start_year int,
	end_year int,
	runtime_minutes int);

CREATE TABLE person_basics
	(person_id char (9) PRIMARY KEY,
	primary_name varchar (110),
	birth_year int,
	death_year int);

CREATE TABLE title_ratings
	(title_id char (9) PRIMARY KEY,
	average_rating numeric(3,1),
	num_votes int
	-- ERROR:  syntax error at or near "FOREIGN"
	FOREIGN KEY title_ratings2title_basics (title_id)
	REFERENCES title_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE);

CREATE TABLE principals
	(title_id char (9),
	person_id char (9)
	-- ERROR:  syntax error at or near "("
	PRIMARY KEY (title_id, person_id)
	FOREIGN KEY principals2title_basics (title_id)
	REFERENCES title_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE);

CREATE TABLE stars
	(person_id char (9),
	title_id char (9)
	-- ERROR:  syntax error at or near "("
	PRIMARY KEY (person_id, title_id)
	FOREIGN KEY stars2person_basics (person_id)
	REFERENCES person_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE
	FOREIGN KEY stars2title_basics (title_id)
	REFERENCES title_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE);

CREATE TABLE directors
	(title_id char (9),
	person_id char (9),
	PRIMARY KEY (title_id, person_id)
	-- ERROR:  syntax error at or near "FOREIGN"
	FOREIGN KEY directors2title_basics (title_id)
	REFERENCES title_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE
	FOREIGN KEY directors2person_basics (person_id)
	REFERENCES person_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE);

CREATE TABLE person_professions
	(person_id char (9),
	profession varchar (30),
	PRIMARY KEY (person_id, profession)
	-- ERROR:  syntax error at or near "FOREIGN"
	FOREIGN KEY person_professions2person_basics (person_id)
	REFERENCES person_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE);

CREATE TABLE title_episodes
	(title_id char (9) PRIMARY KEY,
	parent_title char (9),
	season_num int,
	episode_num int,
	-- ERROR:  syntax error at or near "title_episode2title_basics"
	FOREIGN KEY title_episode2title_basics (title_id)
	REFERENCES title_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE);

CREATE TABLE title_genres
	(title_id char (9),
	genre varchar (20),
	PRIMARY KEY (title_id, genre));

CREATE TABLE writers
	(title_id char (9),
	person_id varchar (20),
	PRIMARY KEY (title_id, person_id)
	-- ERROR:  syntax error at or near "FOREIGN"
	FOREIGN KEY writers2title_basics (title_id)
		ON UPDATE CASCADE
		ON DELETE CASCADE
	FOREIGN KEY writers2person_basics (person_id)
		ON UPDATE CASCADE
		ON DELETE CASCADE);
