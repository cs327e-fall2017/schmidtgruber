/* Query 1: Find all movies/shows that Chris Pratt stars in sorted by rating, from highest to lowest*/
select tb.primary_title, pb.primary_name, tr.average_rating
from title_basics tb join stars s on tb.title_id = s.title_id
join person_basics pb on s.person_id = pb.person_id
join title_ratings tr on tr.title_id = tb.title_id
where pb.primary_name = 'Chris Pratt'
order by tr.average_rating desc;

/* Query 2: Find all movies/shows and their directors with a rating of 9.0 or higher sorted by rating, from highest to lowest */
select tb.primary_title, pb.primary_name, tr.average_rating
from title_basics tb join directors d on tb.title_id = d.title_id
join person_basics pb on pb.person_id = d.person_id
join title_ratings tr on tb.title_id = tr.title_id
where tr.average_rating >= 9.0
order by tr.average_rating desc;

/* Query 3: Find all Drama movies that are 3 hours or longer, sorted by length from longest to shortest*/
select tb.primary_title, tb.runtime_minutes
from title_basics tb join title_genres tg on tb.title_id = tg.title_id
where tb.title_type = 'movie'
and tg.genre = 'Drama'
and tb.runtime_minutes is not null
and tb.runtime_minutes >= 180
order by tb.runtime_minutes desc;

/* Query 4: Find all writers born after 1995 sorted by birth year from most recent to oldest*/
select distinct pb.primary_name, pb.birth_year
from writers w join person_basics pb on w.person_id = pb.person_id
where pb.birth_year is not null
and pb.birth_year >= 1995
order by pb.birth_year desc;

/* Query 5: Find all composers who are already dead and what year they died sorted in alphabetical order*/
select distinct pb.primary_name, pb.death_year
from person_basics pb join person_professions pp on pb.person_id = pp.person_id
where pb.death_year is not null
order by pb.primary_name;

/* Query 6: Find all tv shows that Vin Diesel is in and sort them by alphabetical order*/
select tb.primary_title
from title_basics tb join principals p on tb.title_id = p.title_id
join person_basics pb on p.person_id = pb.person_id
where tb.title_type = 'tvSeries'
and pb.primary_name = 'Vin Diesel'
order by tb.primary_title;

/* Query 7: Find all writers that are also directors, sorted in alphabetical order*/
select distinct pb.primary_name
from person_basics pb join writers w on pb.person_id = w.person_id
join directors d on w.person_id = d.person_id
where pb.birth_year > 1987
order by pb.primary_name;

/* Query 8: Find all movies that have an average rating of 7.0 and higher and have more than 1000 votes sorted by number of votes*/
select tb.primary_title, tr.average_rating, tr.num_votes
from title_basics tb join title_ratings tr on tr.title_id = tb.title_id
where tr.average_rating >= 7.0
and tr.num_votes >= 1000
order by tr.num_votes desc;

/* Query 9: Find all composers who have a movie with a rating of 9.0 or higher and sort by age youngest to oldest*/
select pb.primary_name, pb.birth_year, tr.average_rating
from person_basics pb join person_professions pp on pb.person_id = pp.person_id
join stars s on pp.person_id = s.person_id
join title_ratings tr on tr.title_id = s.title_id
where pp.profession = 'composer'
and tr.average_rating >= 9.0
order by pb.birth_year desc;

/* Query 10: Find all comedy movies and sort by rating from highest to lowest*/
select tb.primary_title, tb.start_year, tr.average_rating
from title_genres tg join title_basics tb on tb.title_id = tg.title_id
join title_ratings tr on tr.title_id = tb.title_id
where tb.title_type = 'movie'
and tg.genre = 'Comedy'
order by tr.average_rating desc;





