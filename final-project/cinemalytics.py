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
    song_id = fields[0]
    song_title = fields[1].strip().upper().encode('utf-8')
    song_duration = float(fields[3])
    return(song_id, song_title, song_duration)


init()
lines = sc.textFile(songs_file)
#This returns an rdd using the function given
rdd = lines.map(parse_songs)




################## links file ##################################

def parse_links_line(line):
    fields = line.split(",")
    movie_id = int(fields[0])
    imdb_id = int(fields[1])
    return (movie_id, imdb_id)


# lookup imdb id
links_lines = sc.textFile(links_file)
#returns an rdd using the function given
rdd_links = links_lines.map(parse_links_line)  # movie_id, imdb_id
# print_rdd(rdd_links, "rdd_links")

##### defines 'rdd_joined' as a result of a join operation on 'rdd_avgs'. the join operation returns a dataset of pairs of elements between 'rdd_avgs' and 'rdd_links' for each key.
rdd_joined = rdd_avgs.join(rdd_links)


# print_rdd(rdd_joined, "movielens_imdb_joined")

# add imdb_id prefix ()
##### defines 'rdd_ratings_by_imdb' as the result of a map transformation on 'rdd_joined'. the map transormation returns a new distributed dataset formed by passing each element of 'rdd_joined' through the function 'add_imdb_id_prefix'.
rdd_ratings_by_imdb = rdd_joined.map(add_imdb_id_prefix)


# print_rdd(rdd_ratings_by_imdb, "rdd_ratings_by_imdb")

def save_rating_to_db(list_of_tuples):
    conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
    conn.autocommit = True
    cur = conn.cursor()

    for tupl in list_of_tuples:
        imdb_id_str, avg_rating = tupl

        # print "imdb_id_str = " + imdb_id_str
        # print "avg_rating = " + str(avg_rating)
        # update_stmt = "update title_ratings set movielens_rating = " + str(avg_rating) + " where title_id = '" + imdb_id_str + "'"
        # print "update_stmt = " + update_stmt + "\n"
        update_stmt = "update title_ratings set movielens_rating = %s where title_id = %s"

        try:
            cur.execute(update_stmt, (avg_rating, imdb_id_str))
        except Exception as e:
            print
            "Error in save_rating_to_db: ", e.message

##### Applies the function 'save_rating_to_db' to each partition of the rdd 'rdd_ratings_by_imdb'
rdd_ratings_by_imdb.foreachPartition(save_rating_to_db)

# free up resources
sc.stop()