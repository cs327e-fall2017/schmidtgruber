create view v_title_financials as select title_id, budget, box_office, avg(budget), avg(box_office)
from Title_Tags tt join Title_Basics tb on tt.title_id = tb.title_id
where box_office < budget
order by budget, box_office desc;