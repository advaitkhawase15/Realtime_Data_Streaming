import uuid
import logging
from pyspark.sql.functions import udf
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users(
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            postcode TEXT,
            email TEXT,
            dob TEXT,
            picture TEXT,
            phone TEXT      
        )
    """)
    print("Table created successfully!")

def insert_data(session, **kwargs):
    print("Inserting data....")

    id = kwargs.get('id') or uuid.uuid4() 
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    dob = kwargs.get('dob')
    picture = kwargs.get('picture')
    phone = kwargs.get('phone')
    
    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, postcode, email, dob, picture, phone)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, first_name, last_name, gender, address, postcode, email, dob, picture, phone))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'Could not insert data due to {e}')

def create_spark_connnection():
    s_conn = None
    try:
        s_conn = SparkSession.builder\
            .appName('SparkDataStreaming')\
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1')\
            .config('spark.cassandra.connection.host', 'localhost')\
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create connection to spark session due to : {e}")
    
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers','localhost:9092')\
            .option('subscribe','user_created')\
            .option('startingOffsets','earliest')\
            .load()
        logging.info("intial dataframe created")
        
    except Exception as e:
        logging.warning(f"Kalfa dataframe could not be created due to : {e}")

    return spark_df

def create_cassandra_connection():
    try:
        cassandra_cluster = Cluster(['localhost'])
        cas_session = cassandra_cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to : {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id",StringType(), False),
        StructField("first_name",StringType(), False),
        StructField("last_name",StringType(), False),
        StructField("gender",StringType(), False),
        StructField("address",StringType(), False),
        StructField("postcode",StringType(), False),
        StructField("email",StringType(), False),
        StructField("dob",StringType(), False),
        StructField("picture",StringType(), False),
        StructField("phone",StringType(), False),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col('value'), schema).alias('data'))\
        .select("data.*")

    print(sel)

    return sel

if __name__ == "__main__":
    # Creating Spark Connnection
    spark_conn = create_spark_connnection()

    if spark_conn is not None:
        # Connect to Kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            insert_data(session)
            
            streaming_query = (selection_df.writeStream
                   .format("org.apache.spark.sql.cassandra")
                   .option('checkpointLocation', '/tmp/checkpoint')
                   .option('keyspace', 'spark_streams')
                   .option('table', 'created_users')
                   .start())
            
            streaming_query.awaitTermination()