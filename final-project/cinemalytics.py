import psycopg2
import sys, os, configparser
from pyspark import SparkConf, SparkContext

log_path = "/home/hadoop/logs/"  # don't change this
aws_region = "us-east-1"  # don't change this
s3_bucket = "cs327e-fall2017-final-project"  # don't change this
persons_file = "s3a://" + s3_bucket + "/cinemalytics/persons.csv"
singer_songs_file = "s3a://" + s3_bucket + "/cinemalytics/singer_songs.csv"
songs_file = "s3a://" + s3_bucket + "/cinemalytics/songs.csv"
title_songs_file = "s3a://" + s3_bucket + "/cinemalytics/title_songs.csv"
titles_file = "s3a://" + s3_bucket + "/cinemalytics/titles.csv"

# global variable sc = Spark Context
sc = SparkContext()

# global variables for RDS connection
rds_config = configparser.ConfigParser()
rds_config.read(os.path.expanduser("~/config"))
rds_database = rds_config.get("default", "database")
rds_user = rds_config.get("default", "user")
rds_password = rds_config.get("default", "password")
rds_host = rds_config.get("default", "host")
rds_port = rds_config.get("default", "port")


def init():
    # set AWS access key and secret account key
    cred_config = configparser.ConfigParser()
    cred_config.read(os.path.expanduser("~/.aws/credentials"))
    access_id = cred_config.get("default", "aws_access_key_id")
    access_key = cred_config.get("default", "aws_secret_access_key")

    # spark and hadoop configuration
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"


################## general utility function ##################################

def print_rdd(rdd, logfile):
    f = open(log_path + logfile, "w")
    #This collects our results so that we can print them
    results = rdd.collect()
    counter = 0
    for result in results:
        counter = counter + 1
        f.write(str(result) + "\n")
        if counter > 30:
            break
    f.close()


################## songs file ##################################

def parse_songs(line):
    fields = line.split("\t")
    song_id = fields[0].strip()
    song_title = fields[1].strip().upper().encode('utf-8')
    song_duration = float(fields[3].strip())
    return(song_id, song_title, song_duration)


init()
lines = sc.textFile(songs_file)
#This returns an rdd using the function given
rdd_songs = lines.map(parse_songs)

################## singer_songs file ##################################

def parse_singer_songs_line(line):
    fields = line.split(",")
    person_id = fields[0].strip()
    song_id = fields[1].strip()
    return (person_id, song_id)


# lookup
singer_songs_lines = sc.textFile(singer_songs_file)

#returns an rdd using the function given
rdd_singer_songs = singer_songs_lines.map(parse_singer_songs_line)  # person_id, song_id

################## persons file ##################################

def parse_persons_line(line):
    fields = line.split("\t")
    person_id = fields[0].strip()
    primary_name = fields[1].strip().upper().encode('utf-8')
    gender = fields[2].strip().upper()
    dob = fields[3].strip()
    return (person_id, primary_name, gender, dob)


# lookup
persons_lines = sc.textFile(persons_file)

#returns an rdd using the function given
rdd_persons = persons_lines.map(parse_persons_line)  # person_id, primary_name, gender, dob

################## title_songs file ##################################

def parse_title_songs_line(line):
    fields = line.split("\t")
    song_id = fields[0].strip()
    movie_id = fields[1].strip
    return (song_id, movie_id)


# lookup
title_songs_lines = sc.textFile(title_songs_file)

#returns an rdd using the function given
rdd_title_songs = title_songs_lines.map(parse_title_songs_line)  # song_id, movie_id

################## titles file ##################################

def parse_titles_line(line):
    fields = line.split("\t")
    movie_id = fields[0].strip()
    imdb_id = fields[1].strip()
    primary_title = fields[2].strip().upper().encode('utf-8')
    original_title = fields[3].strip().upper().encode('utf-8')
    genre = fields[4].strip().upper().encode('utf-8')
    release_year = int(fields[5].strip())
    return (movie_id, imdb_id, primary_title, original_title, genre, release_year)


# lookup
titles_lines = sc.textFile(titles_file)

#returns an rdd using the function given
rdd_titles = titles_lines.map(parse_titles_line)  # movie_id, imdb_id, primary_title, original_title, genre, release_year

####################################

###Songs table###
#load from songs_file

###Singer Songs table###
#load from singer_songs_file

###Person Basics Table###
#want all person_ids from persons that are also in singer songs
# so join singer_songs and persons on person_id
#birth year should be extracted from dob in persons
#person_basics has person_id, primary name, birth year, death year
rdd_singer_songs_join_persons = rdd_singer_songs.join(rdd_persons)
#person_id, song_id, primary_name, gender, dob

###Title Songs table###
#song_id from title_songs file
#title_id from imdb_id in titles file
#join title_songs and titles on movie_id and get the imdb_id(aka title_id) and song_id from that
rdd_title_songs_join_titles = rdd_title_songs.join(rdd_titles)
#song_id, movie_id, imdb_id, primary_title, original_title, genre, release_year



def save_songs_to_db(list_of_songs):
    conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
    conn.autocommit = True
    cur = conn.cursor()

    for songs in list_of_songs:
        song_id, song_title, song_duration = songs

        insert_stmt = "insert into Songs (song_id, song_title, song_duration) values (%s, %s, %s)"

        try:
            cur.execute(insert_stmt, (song_id, song_title, song_duration))
        except Exception as e:
            print
            "Error in save_songs_to_db: ", e.message

def save_singer_songs_to_db(list_of_singer_songs):
    conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host,
                            port=rds_port)
    conn.autocommit = True
    cur = conn.cursor()

    for singer_songs in list_of_singer_songs:
        person_id, song_id = singer_songs

        insert_stmt = "insert into Singer_Songs (person_id, song_id) values (%s, %s)"

        try:
            cur.execute(insert_stmt, (person_id, song_id))
        except Exception as e:
            print
            "Error in save_songs_to_db: ", e.message

def save_person_basics_to_db(list_of_person_basics):
    conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host,
                            port=rds_port)
    conn.autocommit = True
    cur = conn.cursor()

    for person_basics in list_of_person_basics:
        person_id, song_id, primary_name, gender, dob = person_basics
        birth_year = dob[0:4]
        birth_year = int(birth_year)


        insert_stmt = "insert into person_basics (person_id, primary_name, birth_year, death_year) values (%s, %s, %s, %s)"

        try:
            cur.execute(insert_stmt, (person_id, primary_name, birth_year, ''))
        except Exception as e:
            print
            "Error in save_songs_to_db: ", e.message

def save_title_songs_to_db(list_of_title_songs):
    conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host,
                            port=rds_port)
    conn.autocommit = True
    cur = conn.cursor()

    for title_songs in list_of_title_songs:
        song_id, movie_id, imdb_id, primary_title, original_title, genre, release_year = title_songs

        insert_stmt = "insert into Title_Songs (title_id, song_id) values (%s, %s)"

        try:
            cur.execute(insert_stmt, (imdb_id, song_id))
        except Exception as e:
            print
            "Error in save_songs_to_db: ", e.message

##### Applies the function 'save_rating_to_db' to each partition of the rdd 'rdd_ratings_by_imdb'
rdd_songs.foreachPartition(save_songs_to_db)
rdd_singer_songs.foreachPartition(save_singer_songs_to_db)
rdd_singer_songs_join_persons.foreachPartition(save_person_basics_to_db)
rdd_title_songs_join_titles.foreachPartition(save_title_songs_to_db)

# free up resources
sc.stop()