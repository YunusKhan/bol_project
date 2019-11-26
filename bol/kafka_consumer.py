# from pykafka import rdkafka
from pykafka import KafkaClient
import logging
import constants
import kafka_client as kcl
import dill as dill
import pandas as pd
import json
import pickle as cPickle
import pyarrow, fastparquet
from fastparquet import write
import avro
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import os


def get_simple_kafka_consumer(consumer_group='test', topic='bol'):
    """
        Get the kafka simple consumer
        :param : topic : kafka topic, default 'bol'
        :param : consumer_group : consumer group which will be used to route the message
        :return: kafka consumer
    """

    logging.info("Implement getting the kafka simple consumer")
    if not topic:
        logging.debug("Topic not found")
    else:
        try:
            topic_handle = kcl.get_kafka_topic(kcl.get_kafka_client(), topic)
        except ValueError as e:
            logging.debug("Unable to get Kafka topic, check kafka logs %s" % e)

        try:
            return topic_handle.get_simple_consumer(consumer_group)
            # return topic_handle.get_simple_consumer(consumer_group, consumer_timeout_ms=5000)
        except ValueError as e:
            logging.debug("Unable to get Kafka consumer, check kafka logs %s" % e)


def get_balanced_kafka_consumer(consumer_group='test', topic='bol'):
    """
        Get the kafka balanced consumer
        :param : topic : kafka topic, default 'bol'
        :param : consumer_group : consumer group which will be used to route the message
        :return: kafka consumer
    """
    if not topic:
        logging.debug("Topic not found")
    else:
        try:
            topic_handle = kcl.get_kafka_topic(kcl.get_kafka_client(), topic)
        except ValueError as e:
            logging.debug("Unable to get Kafka topic, check kafka logs %s" % e)

        try:
            return topic_handle.get_balanced_consumer(consumer_group)
            # return topic_handle.get_balanced_consumer(consumer_group, consumer_timeout_ms=5000)
        except ValueError as e:
            logging.debug("Unable to get Kafka consumer, check kafka logs %s" % e)


def set_consumer():
    """
        set the kafka consumer if not exists
        :param : topic : kafka topic, default 'bol'
        :return: kafka consumer
    """
    logging.info("Implement setting the kafka consumer")


def get_message_from_consumer(consumer, split='False', kafka_client='None'):
    """
        Get the kafka balanced consumer
        :param : consumer : kafka consumer
        :return: data_frame : data frame containing the message

    """

    logging.info("Implement getting message from kafka consumer")
    data_frame = json.loads('' or 'null')
    if split == 'False':
        for message in consumer:
            temp_data = dill.loads(message.value)
            df_temp = pd.DataFrame(temp_data)
            df_new = df_temp.to_json()
            records = json.loads(df_new).values()
            if data_frame.empty():
                data_frame = records
            else:
                data_frame += records
            # logging.info(records)
            constants.db_instance.boltest.insert(records)

        # return data_frame

    if split == b'split_station_wise':
        topic_stn251 = b'station_251'
        topic_stn350 = b'station_251'
        topic_stn260 = b'station_260'

        for msg in consumer:
            temp_data = cPickle.loads(msg.value)
            df_temp = pd.DataFrame(temp_data)
            df_new = df_temp.to_json()

            # this below set requires to keep creating repetitively on the heap
            # maybe can optimise ??

            if '260' in df_new:
                current_topic = kafka_client.topics[topic_stn260]
                current_producer = current_topic.get_producer()
                current_producer.produce(cPickle.dumps(df_new))
                df_new.to_hdf('data.h5', key='df', mode='w')

            if '350' in df_new:
                current_topic = kafka_client.topics[topic_stn350]
                print(current_topic)
                current_producer = current_topic.get_producer()
                current_producer.produce(cPickle.dumps(df_new))
                df_new.to_hdf('data.h5', key='df', mode='w')

            if '251' in df_new:
                current_topic = kafka_client.topics[topic_stn251]
                print(current_topic)
                current_producer = current_topic.get_producer()
                current_producer.produce(cPickle.dumps(df_new))
                df_new.to_hdf('data.h5', key='df', mode='w')

    if split == b'split_year_wise':
        topic_year_1979 = b'year_1979'
        topic_year_1989 = b'year_1989'
        topic_year_1999 = b'year_1999'
        topic_year_2009 = b'year_2009'
        topic_year_2019 = b'year_2019'

        files = os.listdir(constants.avro_schema_path)
        avro_schema_filename = constants.avro_schema_path + files[0]
        schema = avro.schema.Parse(open(avro_schema_filename, "rb").read().decode('utf-8'))
        writer = avro.io.DatumWriter(schema)
        bytes_writer = avro.io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)

        for msg in consumer:
            temp_data = cPickle.loads(msg.value)
            write('outfile.parq', temp_data)
            df_temp = pd.DataFrame(temp_data)
            df_new = df_temp.to_json()

            if '1979' in df_new:
                writer.write(df_new, encoder)
                raw_bytes = bytes_writer.getvalue()
                current_topic = kafka_client.topics[topic_year_1979]
                current_producer = current_topic.get_producer()
                current_producer.produce(raw_bytes)
                write('outfile.parq', df_new)

            if '1989' in df_new:
                writer.write(df_new, encoder)
                raw_bytes = bytes_writer.getvalue()
                current_topic = kafka_client.topics[topic_year_1989]
                current_producer = current_topic.get_producer()
                current_producer.produce(raw_bytes)
                write('outfile.parq', df_new)

            if '1999' in df_new:
                writer.write(df_new, encoder)
                raw_bytes = bytes_writer.getvalue()
                current_topic = kafka_client.topics[topic_year_1999]
                current_producer = current_topic.get_producer()
                current_producer.produce(raw_bytes)
                write('outfile.parq', df_new)

            if '2009' in df_new:
                writer.write(df_new, encoder)
                raw_bytes = bytes_writer.getvalue()
                current_topic = kafka_client.topics[topic_year_2009]
                current_producer = current_topic.get_producer()
                current_producer.produce(raw_bytes)
                write('outfile.parq', df_new)

            if '2019' in df_new:
                writer.write(df_new, encoder)
                raw_bytes = bytes_writer.getvalue()
                current_topic = kafka_client.topics[topic_year_2019]
                current_producer = current_topic.get_producer()
                current_producer.produce(raw_bytes)
                write('outfile.parq', df_new)

    # split_year_wise_station_wise
    # repeat similar logic for the last piece split year wise and station wise
    if split == b'split_year_wise_station_wise':
        logging.info("similar to previous split, need to implement better logic maybe ")


"""
if __name__ == "__main__":
    k_cons = get_simple_kafka_consumer()
    get_message_from_consumer(k_cons)
"""
