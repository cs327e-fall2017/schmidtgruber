alter table Title_Songs add foreign key(title_id) references Title_Basics(title_id);
alter table Title_Songs add foreign key(song_id) references Songs(song_id);

alter table Singer_Songs add foreign key(person_id) references Person_Basics(person_id);
alter table Singer_Songs add foreign key(song_id) references Songs(song_id);