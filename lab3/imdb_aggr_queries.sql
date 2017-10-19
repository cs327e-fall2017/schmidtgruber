/* 2 inner joins, 1 where clause, 1 having clause
/* Query 1: Actors who have acted in at least 6 titles in a year between the years 1995 - 2000 */
select primary_name as star_name, start_year as year, count(*) as title_count
from title_basics tb join stars s on tb.title_id = s.title_id
join person_basics pb on pb.person_id = s.person_id
where start_year between 1995 and 2000
group by primary_name, start_year
having count(*) >= 6
order by primary_name, start_year, count(*);

/* 3 inner joins, 1 where clause, 1 having clause
/* Query 2: Writers who have written at least 2 Drama titles per year since the year 2000 */
select primary_name as writer_name, start_year as year, count(*) as title_count
from title_basics tb join writers w on tb.title_id = w.title_id
join person_basics pb on pb.person_id = w.person_id
join title_genres tg on tg.title_id = tb.title_id
where genre = 'Drama'
and start_year between 2000 and 2017
group by primary_name, start_year
having count(*) >= 2
order by primary_name, start_year, count(*);

/* 3 inner joins, 1 where clause
/* Query 3: Directors who have directed titles with average ratings between 8 and 10 */
select primary_name as director_name, average_rating as rating, count(*) as title_count
from title_basics tb join directors d on tb.title_id = d.title_id
join person_basics pb on pb.person_id = d.person_id
join title_ratings tr on tr.title_id = d.title_id
where average_rating between 8.0 and 10.0
group by primary_name, average_rating
order by primary_name, average_rating, count(*);

/* 2 inner joins, 1 where clause, 1 having clause
/* Query 4: TV Shows that have at least 100 episodes */
select primary_title as title, count(*) as episode_count
from title_episodes te join title_basics tb on te.parent_title_id = tb.title_id
group by primary_title
having count(*) >= 100
order by count(*) desc, primary_title;

/* Query 5: Principals who have been in atleast 5 titles */
select primary_name as principal_name, count(person_id) as num_of_titles 
from principals p join person_basics pb on p.person_id = pb.person_id
group by primary_name
having count(person_id) >= 5
order by count(*);

 /* Query 6: How many times an actor is in a title and isnt a principal actor */
select primary_name as name, count(*) as not_principal
from person_professions pp left outer join principals p on pp.person_id = p.person_id
join person_basics pb on pb.person_id = pp.person_id
where profession = 'actor'
and p.title_id is null
group by primary_name
order by count(*) desc;

/*  Inner Join Count: DONE
	Outer Join Count: 0/2
	Where Clause Count: DONE
	Having Clause Count: DONE
	Base tables still needing to be accessed: principals, person_professions, 
*/