alter table principals add foreign key(title_id) references title_basics(title_id);
alter table principals add foreign key(person_id) references person_basics(person_id);

alter table stars add foreign key(person_id) references person_basics(person_id);
alter table stars add foreign key(title_id) references title_basics(title_id);

alter table directors add foreign key(title_id) references title_basics(title_id);
alter table directors add foreign key(person_id) references person_basics(person_id);

alter table writers add foreign key(title_id) references title_basics(title_id);
alter table writers add foreign key(person_id) references person_basics(person_id);