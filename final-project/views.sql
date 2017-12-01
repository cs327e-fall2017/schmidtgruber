create view v_songs as select song_duration, count(*)
from songs
group by song_duration
order by song_duration desc;

create view v_singer_songs as select primary_name, ss.person_id, count(*)
from singer_songs ss join person_basics pb on ss.person_id = pb.person_id
group by primary_name, ss.person_id
order by count(ss.person_id) desc;

create view v_title_songs as select song_id, count(*)
from title_songs
group by song_id
order by count(song_id) desc;