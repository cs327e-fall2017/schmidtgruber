/* Part A */
/* Query 1 */
select title_type, start_year as year, genre, count(*) as appalling_titles 
from title_basics tb join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating <= 2.0
group by title_type, start_year, genre;

/* Query 2 */
select title_type, year, genre, count(*) as average_titles
from title_basics tb join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating between 2.1 and 7.9
group by title_type, start_year, genre;

/* Query 3 */
select title_type, year, genre, count(*) as outstanding_titles
from title_basics tb join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating >= 8.0
group by title_type, start_year, genre;

/* Part B */
/* Query 1 */
create table Title_Rating_Facts_Appalling as 
select title_type, start_year as year, genre, count(*) as appalling_titles 
from title_basics tb join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating <= 2.0
group by title_type, start_year, genre;

/* Query 2 */
create table Title_Rating_Facts_Average as 
select title_type, year, genre, count(*) as average_titles
from title_basics tb join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating between 2.1 and 7.9
group by title_type, start_year, genre;

/* Query 3 */
create table Title_Rating_Facts_Outstanding as 
select title_type, year, genre, count(*) as outstanding_titles
from title_basics tb join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating >= 8.0
group by title_type, start_year, genre;

/* Part C */
create table Title_Rating_Facts as select title_type, year, genre, appalling_titles, average_titles, outstanding_titles
from Title_Rating_Facts_Appalling appal full outer join Title_Rating_Facts_Average avg on appal.title_type = avg.title_type
and on appal.year = avg.year and on appal.genre = avg.genre
full outer join Title_Rating_Facts_Outstanding out on appal.title_type = out.title_type
and on appal.year = out.year and on appal.genre = out.genre

/* Part D */
update Title_Rating_Facts set appalling_titles = 0 where appalling_titles = null;
update Title_Rating_Facts set average_titles = 0 where appalling_titles = null;
update Title_Rating_Facts set outstanding_titles = 0 where appalling_titles = null;

/* Part E ?*/
delete from Title_Rating_Facts
where title_type not distinct

alter table Title_Rating_Facts add primary key (title_type, year, genre)

/* Part F */
create view v_outstanding_titles_by_year_genre as 
select year, genre, count(*)
from Title_Rating_Facts
where outstanding_titles > 0
and year > 1930
group by year, genre
limit 100;