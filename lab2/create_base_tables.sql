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
	FOREIGN KEY title_ratings2title_basics (title_id)
	REFERENCES title_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE);

CREATE TABLE principals
	(title_id char (9),
	person_id char (9)
	PRIMARY KEY (title_id, person_id)
	FOREIGN KEY principals2title_basics (title_id)
	REFERENCES title_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE);

CREATE TABLE stars
	(person_id char (9),
	title_id char (9)
	PRIMARY KEY (person_id, title_id)
	FOREIGN KEY stars2person_basics (person_id)
	REFERENCES person_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE
	FOREIGN KEY stars2title_basics (title_id)
	REFERENCES title_basics
		ON UPDATE CASCADE
		ON DELETE CASCADE);