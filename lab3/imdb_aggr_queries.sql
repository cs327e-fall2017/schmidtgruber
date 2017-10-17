/* 2 inner joins ? 1 where clause, 1 having clause
/* Actors who have acted in at least 1 titles per year between the years 1995 - 2000 */
select primary_name as star_name, start_year as year, count(*) as title_count
from title_basics tb join stars s on tb.title_id = s.title_id
join person_basics pb on pb.person_id = d.person_id
where start_year between 1995 and 2000
group by primary_name, start_year
having count(*) >= 1
order by start_year, count(*);