CREATE EXTERNAL TABLE IF NOT EXISTS imdb.directors (
  `title_id` string,
  `person_id` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/directors/'
TBLPROPERTIES ('has_encrypted_data'='false');