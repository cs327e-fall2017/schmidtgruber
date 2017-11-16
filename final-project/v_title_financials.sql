create view v_title_financials as select start_year, avg(box_office - budget)
from Title_Financials tf join title_basics tb on tf.title_id = tb.title_id
where tb.start_year between 2000 and 2010
and tf.box_office > tf.budget
group by start_year
order by start_year desc;