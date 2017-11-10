create view v_title_tags as select tag, count(tag)
from Title_Tags tt join Title_Basics tb on tt.title_id = tb.title_id
group by tag
order by count(tag) desc;