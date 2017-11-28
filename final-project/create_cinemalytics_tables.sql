create table Songs (
	song_id char(8) primary key,
	song_title varchar(50),
	song_duration numeric(5,2)
);

create table Title_Songs (
	title_id char(8),
	song_id char(8),
	primary key(title_id, song_id)
);

create table Singer_Songs (
	person_id char(8),
	song_id char(8),
	primary key(person_id, song_id)
);