create view v_movielens_rating as select genre, avg(movielens_rating)
from title_ratings tr join title_genres tg on tr.title_id = tg.title_id
group by genre
order by genre desc;