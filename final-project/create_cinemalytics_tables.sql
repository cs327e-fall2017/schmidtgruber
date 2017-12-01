create table Songs (
	song_id varchar(20) primary key,
	song_title varchar(110),
	song_duration numeric(5,2)
);

create table Title_Songs (
	title_id varchar(300),
	song_id varchar(300),
	primary key(title_id, song_id)
);

create table Singer_Songs (
	person_id varchar(20),
	song_id varchar(20),
	primary key(person_id, song_id)
);