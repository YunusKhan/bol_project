# constants file
# this file contains details of all constants that are used
# usually in production we may have specific files, but in
# this case I have taken a single file with all constants for convenience
import datetime as dt
import logging
import sys
import pymongo

# logging details
# logfilename = dt.datetime.now().strftime("%d.%b %Y %H:%M:%S") + '.log'
logger = logging.getLogger()
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s - line %(lineno)d"))
log_handler.setLevel(logging.DEBUG)
logger.addHandler(log_handler)
logger.setLevel(logging.DEBUG)

# source file location
source_file_path = '../data_src/weather-data/'

# avro serialization schema details
avro_schema_path = '../schema/'

# ERROR CODES
RESET_CODE = 0
ALL_OK = 100
KAFKA_DATA_FRAME_NOT_AVL = 101
KAFKA_NO_TOPIC_FOUND = 102
KAFKA_NO_PRODUCER_AVL = 103
KAFKA_NO_CONSUMER_AVL = 104
KAFKA_MESSAGE_NOT_SEND_PRODUCER = 105

EMPTY_DATA_FRAME = 151
# kafka connection parameters
kafka_hostname_port = "127.0.0.1:9092"
kakfa_consumer_groupname = b'test'

# parquet file connection details


# mongo db connection details
db_path = "mongodb://localhost:27017/"
db_instance_name = "bol"
db_name = "boltest"
client_name = pymongo.MongoClient(db_path)
db_instance = client_name[db_instance_name]
collection_name = db_instance[db_name]

# hbase connection details


# spark connection details
