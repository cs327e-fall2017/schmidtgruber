/* 2 inner joins, 1 where clause, 1 having clause
/* Actors who have acted in at least 1 titles per year between the years 1995 - 2000 */
select primary_name as star_name, start_year as year, count(*) as title_count
from title_basics tb join stars s on tb.title_id = s.title_id
join person_basics pb on pb.person_id = s.person_id
where start_year between 1995 and 2000
group by primary_name, start_year
having count(*) >= 1
order by start_year, count(*);

/* 2 inner joins, 1 where clause, 1 having clause
/* Query 2: Writers who have written at least 2 Drama titles per year since the year 2000 */
select primary_name as writer_name, start_year as year, count(*) as title_count
from title_basics tb join writers w on tb.title_id = w.title_id
join person_basics pb on pb.person_id = w.person_id
where start_year between 2000 and 2017
and genre = "Drama"
group by primary_name, start_year
having count(*) >= 2
order by start_year, count(*);

/* 3 inner joins, 1 where clause
/* Query 3: Directors who have directed titles with average ratings between 8 and 10 */
select primary_name as director_name, average_rating as rating, count(*) as title_count
from title_basics tb join directors d on tb.title_id = d.title_id
join person_basics pb on pb.person_id = d.person_id
join title_ratings tr on tr.title_id = d.title_id
where rating between 8.0 and 10.0
group by primary_name, average_rating
order by average_rating, count(*);

/* 2 inner joins, 1 where clause, 1 having clause
/* Query 4: TV Shows that have at least 100 episodes and a rating of 5 or higher */
select primary_title as title_name, average_rating as rating, start_year as year, count(*) as episode_count
from title_basics tb join title_episodes te on tb.title_id = te.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating >= 5.0
group by title_name, episodes
having count(*) >= 100
order by episodes, count(*);

/*  Inner Join Count: DONE
	Outer Join Count: 0/2
	Where Clause Count: DONE
	Having Clause Count: DONE
	Bast tables still needing to be accessed: principals, person_professions, title_episodes, 