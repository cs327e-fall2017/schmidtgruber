delete from Title_Songs where title_id not in (select tb.title_id from Title_Songs ts join title_basics tb on ts.title_id = tb.title_id);

delete from Singer_Songs where person_id not in (select ss.person_id from Singer_Songs ss join person_basics pb on ss.person_id = pb.person_id);