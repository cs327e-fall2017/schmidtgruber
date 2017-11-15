create index select_stmt_1_index on title_basics(upper(primary_title), start_year);

create index select_stmt_2_index on title_basics(upper(primary_title), start_year) where title_type != 'tvEpisode';

create index select_stmt_3_index on title_genres(genre);