import psycopg2
import sys, os, configparser, csv
from pyspark import SparkConf, SparkContext

log_path = "/home/hadoop/logs/"  # don't change this
aws_region = "us-east-1"  # don't change this
s3_bucket = "cs327e-fall2017-final-project"  # don't change this
the_numbers_files = "s3a://" + s3_bucket + "/the-numbers/*"  # dataset for milestone 3

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
    results = rdd.collect()
    counter = 0
    for result in results:
        counter = counter + 1
        f.write(str(result) + "\n")
        if counter > 30:
            break
    f.close()


################## process the-numbers dataset #################################

def parse_line(line):
    # add logic for parsing and cleaning the fields as specified in step 4 of assignment sheet
    fields = line.split("\t")
    release_year = int(fields[0][-4:])
    movie_title = fields[1].strip().upper().replace("'", "").encode('utf-8')

    genre = fields[2].strip()
    if genre == "Thriller/Suspense":
        genre = "Thriller"
    elif genre == "Black Comedy":
        genre = "Comedy"
    elif genre == "Romantic Comedy":
        genre = "Romance"
    genre = genre.upper().encode('utf-8')

    budget = fields[3].strip().replace("$", "").replace(",", "").replace("\"", "")
    if budget == "":
        budget = -1
    else:
        budget = int(budget)

    box_office = fields[4].strip().replace("$", "").replace(",", "").replace("\"", "")
    if box_office == "":
        box_office = -1
    else:
        box_office = int(box_office)

    return (release_year, movie_title, genre, budget, box_office)


init()
base_rdd = sc.textFile(the_numbers_files)
mapped_rdd = base_rdd.map(parse_line)
print_rdd(mapped_rdd, "mapped_rdd")


def save_to_db(list_of_tuples):
    conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
    conn.autocommit = True
    cur = conn.cursor()

    # add logic to look up the title_id in the database as specified in step 5 of assignment sheet
    # add logic to write out the financial record to the database as specified in step 5 of assignment sheet
    for tupl in list_of_tuples:
        release_year, movie_title, genre, budget, box_office = tupl
        select_stmt = "select count(*) from title_basics where upper(primary_title) = %s and start_year = %s"

        try:
            cur.execute(select_stmt, (movie_title, release_year))
            row = cur.fetchone()

            if row[0] > 1:
                if box_office > 0:
                    select_stmt = "select title_id from title_basics where upper(primary_title) = %s and start_year = %s and title_type != 'tvEpisode'"
                    cur.execute(select_stmt, (movie_title, release_year))

                else:
                    select_stmt = "select tb.title_id from title_basics tb join title_genres tg on tb.title_id = tg.title_id where upper(primary_title) = %s and start_year = %s and genre = %s"
                    cur.execute(select_stmt, (movie_title, release_year, genre))

                row2 = cur.fetchone()
                if row2 is not None:
                    title_id = row2[0]
                else:
                    continue

                insert_stmt = "insert into Title_Financials (title_id, budget, box_office) values (%s, %s, %s)"
                cur.execute(insert_stmt, (title_id, budget, box_office))
            elif row[0] == 1:
                select_stmt = "select title_id from title_basics where upper(primary_title) = %s and start_year = %s"
                cur.execute(select_stmt, (movie_title, release_year))
                row3 = cur.fetchone()
                if row3 is not None:
                    title_id = row3[0]
                else:
                    continue

                insert_stmt = "insert into Title_Financials (title_id, budget, box_office) values (%s, %s, %s)"
                cur.execute(insert_stmt, (title_id, budget, box_office))


        except Exception as e:
            print("Exception: " + e.message)


    cur.close()
    conn.close()


mapped_rdd.foreachPartition(save_to_db)

# free up resources
sc.stop()