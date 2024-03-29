create materialized view v_actors_six_titles as
select primary_name as star_name, start_year as year, count(*) as title_count
from title_basics tb join stars s on tb.title_id = s.title_id
join person_basics pb on pb.person_id = s.person_id
where start_year between 1995 and 2000
group by primary_name, start_year
having count(*) >= 6
order by primary_name, start_year, count(*);

create materialized view v_drama_writers as
select primary_name as writer_name, start_year as year, count(*) as title_count
from title_basics tb join writers w on tb.title_id = w.title_id
join person_basics pb on pb.person_id = w.person_id
join title_genres tg on tg.title_id = tb.title_id
where genre = 'Drama'
and start_year between 2000 and 2017
group by primary_name, start_year
having count(*) >= 2
order by primary_name, start_year, count(*);

create materialized view v_director_ratings as
select primary_name as director_name, average_rating as rating, count(*) as title_count
from title_basics tb join directors d on tb.title_id = d.title_id
join person_basics pb on pb.person_id = d.person_id
join title_ratings tr on tr.title_id = d.title_id
where average_rating between 8.0 and 10.0
group by primary_name, average_rating
order by primary_name, average_rating, count(*);

create materialized view v_tv_shows as
select primary_title as title, count(*) as episode_count
from title_episodes te join title_basics tb on te.parent_title_id = tb.title_id
group by primary_title
having count(*) >= 100
order by count(*) desc, primary_title;

create materialized view v_writers_not_directors as
select primary_name as name, count(*) as num_titles
from writers w left outer join directors d on w.person_id = d.person_id
join person_basics pb on pb.person_id = w.person_id
join stars s on w.person_id = s.person_id
where d.title_id is null
group by primary_name
order by count(*) desc;

create materialized view v_not_principal as
select primary_name as name, count(*) as not_principal
from person_professions pp left outer join principals p on pp.person_id = p.person_id
join person_basics pb on pb.person_id = pp.person_id
where profession = 'actor'
and p.title_id is null
group by primary_name
having count(*) > 35
order by count(*) desc;