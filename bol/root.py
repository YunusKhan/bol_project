import logging

import constants
import kafka_client as kcl
import kafka_consumer as kcon
import kafka_producer as kpro
import pyarrow.parquet as pq

import os
import pandas as pd

import dill as dill
import json
import pickle as cPickle


def main():
    """
    main
    :param : None
    :return: ERROR CODE
    """
    logging.info(" Starting Execution from root file... ")
    logging.warning(" Ensure Kafka has at least 3 brokers otherwise you will run into "
                    "broker issues while producing consuming multiple messages. Single"
                    "broker cannot handle multiple requests at the same time, it will"
                    "result in file sharing IO errors")

    df = stream_whole_data_set()
    station_wise = stream_station_wise_data_set(df)
    year_wise = year_wise_data_set(df)
    year_wise_station_wise = year_wise_station_wise_data_set(df)
    return constants.ALL_OK


def get_files_list():
    """
    return files in the directory. These are the input source
    :param : None
    :return: list of files
    """

    try:
        return os.listdir(constants.source_file_path)
    except IOError as e:
        err_no, str_error = e.args
        logging.error("I/O error({0}): {1}".format(err_no, str_error))


def stream_whole_data_set():
    """
    convert input to a dataframe,
    push the entire dataframe in a kafka producer after serialization
    The message is to be consumed by a consumer and push in a mongo database
    :param : None
    :return: ERROR CODE
    """

    logging.info("Will push whole data set into "
                 "a kafka producer. It will then be consumed into by a queue"
                 " and so on. This will be pushed row wise in a mongo db collection"
                 "called boltest .")

    try:
        list_of_files = get_files_list()
    except IOError as e:
        err_no, str_error = e.args
        logging.error("I/O error({0}): {1}".format(err_no, str_error))

    list_of_files = get_files_list()
    if not list_of_files:
        logging.debug("No data source found, returning back to root")
        exit(1)

    data_frame_holder = []  # define empty list to hold all files

    for f in list_of_files:
        try:
            data_frame_holder.append(pd.read_csv(constants.source_file_path + f,
                                                 index_col=None,
                                                 parse_dates=['YYYYMMDD'],
                                                 header=0,
                                                 sep='\s*,\s*'))
        except ValueError as e:
            logging.debug("Exception Raised, check data frame properly loading or not for source directory, %s" % e)
            raise

    try:
        df = pd.DataFrame(pd.concat(data_frame_holder, axis=0))
    except ValueError as e:
        logging.debug("Unable to construct Final Data Frame for processing, %s" % e)
        raise

    try:
        df = df.fillna(method='ffill')
        df = df.dropna(axis=0)
        logging.info(df)
    except ValueError as e:
        logging.debug("Unable to forward fill Final Data Frame for processing, %s" % e)
        logging.debug("Unable to backward fill or NaN options ")
        raise

    list_of_columns = df.head()
    if len(list_of_columns) == 0:
        logging.debug("List of columns are not available, check if df.head() returns anything useful")
        return constants.EMPTY_DATA_FRAME

    # create a kafka connection
    try:
        kafka_client = kcl.get_kafka_client()
    except ValueError as e:
        logging.debug("Unable to get Kafka Client, check zk and kafka "
                      "server up and port availability , %s" % e)

    logging.info("Kafka Client is set to %s" % kafka_client)

    try:
        kafka_topic = kcl.get_kafka_topic(kafka_client, 'bol')
    except ValueError as e:
        logging.debug("Unable to get Kafka topic, check if topic needs to be "
                      "created separately, or generated , %s" % e)

    ret_code = kpro.send_message_to_producer(kafka_topic, df, 'simple', 'test', 'pickle')
    logging.info(ret_code)

    if ret_code == constants.ALL_OK:
        logging.debug("Producer successfully sent message")
        ret_code = constants.RESET_CODE
    else:
        logging.debug("Producer not successfully produced messages , check kafka logs")

    '''with kafka_topic.get_sync_producer() as producer:
        for row in df.itertuples():
            # producer.produce(pickle.dumps(row))
            producer.produce(dill.dumps(row))
    logging.debug("Finished producer work ")'''

    try:
        consumer = kcon.get_simple_kafka_consumer("test", kafka_topic.name)
    except ValueError as e:
        logging.debug("Unable to get consumer, check kafka logs, %s" % e)

    # may not run consumer from the program
    # running into a library issue where consumer does not stop
    # and return control to python program after consuming all messages
    # this is probably because rdkafka needs to be built on win10
    # using vc++ 17 or something compatible
    # then build python compatible obj files for this
    # this might take time
    # if possible will debug some other way
    # otherwise will run a standalone consumer program on console
    # keep it running on console and consume messages
    # this issue is not seen with the producer
    # it appears to be specific issue with library
    # sorry for this incorrect way of making it work
    # but due to time constraints this is being done that way

    '''try:
        ret_code = kcon.get_message_from_consumer(consumer)
    except ValueError as e:
        logging.debug("Unable to consume messages, check kafka logs , %s" % e)
'''
    if ret_code == constants.ALL_OK:
        logging.debug("Consumer successfully sent message")
    else:
        logging.debug("Consumer not successfully consumed messages, check kafka logs")

    # while True:
    """for msg in consumer:
        if not msg:
            consumer.stop()
        tempdata = dill.loads(msg.value)
        dftemp = pd.DataFrame(tempdata)
        dfnew = dftemp.to_json()
        records = json.loads(dfnew).values()
        constants.db_instance.boltest.insert(records)
    """
    return df


def station_wise_split_to_producer(df):
    """
    convert input to a dataframe,
    push the entire dataframe in a kafka producer after serialization
    The message is to be consumed by a consumer and push in a hdfs table
    :param : df : input data frame
    :return: ERROR CODE
    """

    logging.info("Pass entire df to producer"
                 "Then producer will move it to consumer"
                 "Consumer will check which station it is"
                 "And pass it back to the kafka producer"
                 "via the appropriate queue"
                 "then insert in some db or flat file or hdfs store ")
    # new topic names

    try:
        kafka_client = kcl.get_kafka_client()
    except ValueError as e:
        logging.debug("Unable to get Kafka Client, check zk and kafka "
                      "server up and port availability , %s" % e)
        raise

    logging.info("Kafka Client is set to %s" % kafka_client)

    try:
        kafka_topic = kcl.get_kafka_topic(kafka_client, 'bol_sample_Q2')
        # print(kafka_topic)
    except ValueError as e:
        logging.debug("Unable to get Kafka topic, check if topic needs to be "
                      "created separately, or generated , %s" % e)
    # producer code
    ret_code = kpro.send_message_to_producer(kafka_topic, df, 'simple', 'test_q1', 'pickle')
    logging.info(ret_code)

    if ret_code == constants.ALL_OK:
        logging.debug("Producer successfully sent message")
        ret_code = constants.RESET_CODE
    else:
        logging.debug("Producer not successfully produced messages , check kafka logs")

    # consumer code

    try:
        consumer = kcon.get_balanced_kafka_consumer("test_Q2", kafka_topic.name)
    except ValueError as e:
        logging.debug("Unable to get consumer, check kafka logs, %s" % e)

    try:
        ret_code = kcon.get_message_from_consumer(consumer, 'split_station_wise', kafka_client)
    except ValueError as e:
        logging.debug("Unable to consume messages, check kafka logs , %s" % e)

    if ret_code == constants.ALL_OK:
        logging.debug("Consumer successfully sent message")
    else:
        logging.debug("Consumer not successfully consumed messages, check kafka logs")

    return constants.ALL_OK


def stream_station_wise_data_set(df):
    """
    convert input to a dataframe,
    push the entire dataframe in a kafka producer after serialization
    consume, sort the data frame station wise and push back into another
    producer specific to stations
    :param : df : input data frame
    :return: ERROR CODE
    """
    logging.info("Will start splitting data set into station wise and then stream it into "
                 "a kafka producer. It will then be consumed into by a queue and so on")

    df = station_wise_split_to_producer(df)
    return constants.ALL_OK


def year_wise_split_to_producer(df):
    """
    convert input to a dataframe,
    push the entire dataframe in a kafka producer after serialization
    consume, sort the data frame year wise and push back into another
    producer specific to stations
    :param : df : input data frame
    :return: ERROR CODE
    """

    logging.info("Pass entire df to producer"
                 "Then producer will move it to consumer"
                 "Consumer will check which station it is"
                 "And pass it back to the kafka producer"
                 "via the appropriate queue"
                 "then insert in some db or flat file or hdfs store ")
    # new topic names

    try:
        kafka_client = kcl.get_kafka_client()
    except ValueError as e:
        logging.debug("Unable to get Kafka Client, check zk and kafka "
                      "server up and port availability , %s" % e)
        raise

    logging.info("Kafka Client is set to %s" % kafka_client)

    try:
        kafka_topic = kcl.get_kafka_topic(kafka_client, 'bol_sample_Q3')
        # print(kafka_topic)
    except ValueError as e:
        logging.debug("Unable to get Kafka topic, check if topic needs to be "
                      "created separately, or generated , %s" % e)
    # producer code
    ret_code = kpro.send_message_to_producer(kafka_topic, df, 'simple', 'test_q1', 'pickle')
    logging.info(ret_code)

    if ret_code == constants.ALL_OK:
        logging.debug("Producer successfully sent message")
        ret_code = constants.RESET_CODE
    else:
        logging.debug("Producer not successfully produced messages , check kafka logs")

    # consumer code

    try:
        consumer = kcon.get_balanced_kafka_consumer("test_Q3", kafka_topic.name)
    except ValueError as e:
        logging.debug("Unable to get consumer, check kafka logs, %s" % e)

    try:
        ret_code = kcon.get_message_from_consumer(consumer, 'split_year_wise', kafka_client)
    except ValueError as e:
        logging.debug("Unable to consume messages, check kafka logs , %s" % e)

    if ret_code == constants.ALL_OK:
        logging.debug("Consumer successfully sent message")
    else:
        logging.debug("Consumer not successfully consumed messages, check kafka logs")

    return constants.ALL_OK


def year_wise_data_set(df):
    """
    convert input to a dataframe,
    push the entire dataframe in a kafka producer after serialization
    consume, sort the data frame year wise and push back into another
    producer specific to stations
    :param : df : input data frame
    :return: ERROR CODE
    """
    logging.info("Will start splitting data set into station wise and then stream it into "
                 "a kafka producer. It will then be consumed into by a queue and so on")

    df = year_wise_split_to_producer(df)
    return constants.ALL_OK

def year_wise_station_wise_to_producer(df):
    """
    convert input to a dataframe,
    push the entire dataframe in a kafka producer after serialization
    consume, sort the data frame year & station wise and push back into another
    producer specific to stations
    :param : df : input data frame
    :return: ERROR CODE
    """

    logging.info("Pass entire df to producer"
                 "Then producer will move it to consumer"
                 "Consumer will check which station it is"
                 "And pass it back to the kafka producer"
                 "via the appropriate queue"
                 "then insert in some db or flat file or hdfs store ")
    # new topic names

    try:
        kafka_client = kcl.get_kafka_client()
    except ValueError as e:
        logging.debug("Unable to get Kafka Client, check zk and kafka "
                      "server up and port availability , %s" % e)
        raise

    logging.info("Kafka Client is set to %s" % kafka_client)

    try:
        kafka_topic = kcl.get_kafka_topic(kafka_client, 'bol_sample_Q3')
        # print(kafka_topic)
    except ValueError as e:
        logging.debug("Unable to get Kafka topic, check if topic needs to be "
                      "created separately, or generated , %s" % e)
    # producer code
    ret_code = kpro.send_message_to_producer(kafka_topic, df, 'simple', 'test_q1', 'pickle')
    logging.info(ret_code)

    if ret_code == constants.ALL_OK:
        logging.debug("Producer successfully sent message")
        ret_code = constants.RESET_CODE
    else:
        logging.debug("Producer not successfully produced messages , check kafka logs")

    # consumer code

    try:
        consumer = kcon.get_balanced_kafka_consumer("test_Q3", kafka_topic.name)
    except ValueError as e:
        logging.debug("Unable to get consumer, check kafka logs, %s" % e)

    try:
        ret_code = kcon.get_message_from_consumer(consumer, 'split_year_wise_station_wise', kafka_client)
    except ValueError as e:
        logging.debug("Unable to consume messages, check kafka logs , %s" % e)

    if ret_code == constants.ALL_OK:
        logging.debug("Consumer successfully sent message")
    else:
        logging.debug("Consumer not successfully consumed messages, check kafka logs")

    return constants.ALL_OK


def year_wise_station_wise_data_set(df):
    """
    convert input to a dataframe,
    push the entire dataframe in a kafka producer after serialization
    consume, sort the data frame year wise and push back into another
    producer specific to stations
    :param : df : input data frame
    :return: ERROR CODE
    """
    logging.info("Will start splitting data set into station wise and then stream it into "
                 "a kafka producer. It will then be consumed into by a queue and so on")

    df = year_wise_station_wise_to_producer(df)
    return constants.ALL_OK


if __name__ == "__main__":
    main()
