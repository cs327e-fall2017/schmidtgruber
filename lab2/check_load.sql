--4486305
--8108929
--759962
--25139892
select count(*) from title_basics;
select count(*) from person_basics;
select count(*) from title_ratings;
select count(*) from principals;
select count(*) from stars;
select count(*) from directors;
select count(*) from person_professions;
select count(*) from title_episodes;
select count(*) from title_genres;
select count(*) from writers;

select title_id from title_basics limit(5);
select title_type from title_basics limit(5);
select primary_title from title_basics limit(5);
select original_title from title_basics limit(5);
select is_adult from title_basics limit(5);
select start_year from title_basics limit(5);
select end_year from title_basics limit(5);
select runtime_minutes from title_basics limit(5);

select person_id from person_basics limit(5);
select primary_name from person_basics limit(5);
select birth_year from person_basics limit(5);
select death_year from person_basics limit(5);

select title_id from title_ratings limit(5);
select average_ratings from title_ratings limit(5);
select num_votes from title_ratings limit(5);

select title_id from principals limit(5);
select person_id from principals limit(5);

select person_id from stars limit(5);
select title_id from stars limit(5);

select title_id from directors limit(5);
select person_id from directors limit(5);

select person_id from person_professions limit(5);
select profession from person_professions limit(5);

select title_id from title_episodes limit(5);
select parent_title_id from title_episodes limit(5);
select season_num from title_episodes limit(5);
select episode_num from title_episodes limit(5);

select title_id from title_genres limit(5);
select genre from title_genres limit(5);

select title_id from writers limit(5);
select person_id from writers limit(5);