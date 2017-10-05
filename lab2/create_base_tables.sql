create table title_basics(
	title_id varchar(10) primary key,
	title_type varchar(20),
	primary_title varchar(300),
	original_title varchar(300),
	is_adult boolean,
	start_year int,
	end_year int,
	runtime_minutes int
);

create table person_basics(
	person_id varchar(10) primary key,
	primary_name varchar(110),
	birth_year int,
	death_year int
);

create table title_ratings(
	title_id varchar(10) primary key,
	average_rating numeric(3,1),
	num_votes int,
	foreign key(title_id) references title_basics
);

create table principals(
	title_id varchar(10),
	person_id varchar(10),
	primary key(title_id, person_id),
	foreign key(title_id) references title_basics
);

create table stars(
	person_id varchar(10),
	title_id varchar(10),
	primary key(person_id, title_id),
	foreign key(person_id) references person_basics,
	foreign key(title_id) references title_basics
);

create table directors(
	title_id varchar(10),
	person_id varchar(10),
	primary key(title_id, person_id),
	foreign key(title_id) references title_basics,
	foreign key(person_id) references person_basics
);

create table person_professions(
	person_id varchar(10),
	profession varchar(30),
	primary key(person_id, profession),
	foreign key(person_id) references person_basics
);

create table title_episodes(
	title_id varchar(10) primary key,
	parent_title varchar(10),
	season_num int,
	episode_num int,
	foreign key(title_id) references title_basics
);

create table title_genres(
	title_id varchar(10),
	genre varchar(20),
	primary key(title_id, genre)
);

create table writers(
	title_id varchar(10),
	person_id varchar(20),
	primary key(title_id, person_id),
	foreign key(title_id) references title_basics,
	foreign key(person_id) references person_basics
);
