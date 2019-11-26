# from pykafka import rdkafka
from pykafka import KafkaClient
import logging
import dill as dill
import constants


def get_topic():
    """
    Get the kafka topic

    :return: kafka topic
    """
    logging.info("Implement getting the kafka topic")


def set_topic():
    """
    Set the kafka topic

    :return: kafka topic
    """
    logging.info("Implement setting the kafka topic")


def get_simple_producer(kafka_topic):
    """
    Get the kafka simple producer
    :param : kafka_topic : kafka topic
    :return: kafka producer
    """
    logging.info("Implement getting the kafka simple producer")
    logging.info(kafka_topic)
    try:
        return kafka_topic.get_producer()

    except ValueError as e:
        logging.debug("Unable to get Kafka producer, check kafka logs %s" % e)


def get_synchronised_producer(kafka_topic):
    """
    Get the kafka synchronised producer
    :param : kafka_topic : kafka topic
    :return: kafka producer
    """
    logging.info("Implement getting the kafka synchronised producer")
    logging.info(kafka_topic)
    try:
        return kafka_topic.get_sync_producer()
        # return kafka_topic.get_producer()

    except ValueError as e:
        logging.debug("Unable to get Kafka producer, check kafka logs %s" % e)


def set_producer():
    """
    Set a new  kafka simple producer

    :return: kafka producer
    """
    logging.info("Implement setting the kafka producer")


def send_message_to_producer(kafka_topic, data_frame, producer_type, consumer_group, serialization_scheme):
    """
    Set a message to the producer

    :param : kafka_topic : kafka topic
    :param : data_frame : data frame containing the message
    :param : producer_type : simple or balanced producer
    :param : consumer_group : simple or balanced producer
    :param : serialization_scheme : pickle, dill, avro or kyro, have to implement
    :return: error code
    """
    logging.info("Implement sending message to kafka producer")

    if data_frame.empty:
        logging.debug("no data to send to producer, exiting producer")
        return constants.KAFKA_DATA_FRAME_NOT_AVL

    if not kafka_topic:
        logging.debug("Topic has not been passed correctly")
        return constants.KAFKA_NO_TOPIC_FOUND

    if producer_type == 'simple' or not producer_type:
        try:
            pr = get_simple_producer(kafka_topic)
        except ValueError as e:
            logging.debug("Unable to get Kafka producer, check kafka logs %s" % e)
            return constants.KAFKA_NO_PRODUCER_AVL
    else:
        try:
            pr = get_synchronised_producer(kafka_topic)
        except ValueError as e:
            logging.debug("Unable to get Kafka producer, check kafka logs %s" % e)
            return constants.KAFKA_NO_PRODUCER_AVL

    if not consumer_group:
        logging.debug("consumer_group has not been set, using default")
        consumer_group = b'test'

    if not serialization_scheme:
        logging.debug("serialization_scheme has not been set, using default pickle")
        serialization_scheme = b'dill'

    with pr as producer:
        for row in data_frame.itertuples():
            try:
                # producer.produce(pickle.dumps(row))
                producer.produce(dill.dumps(row))
            except ValueError as e:
                logging.debug("Unable to send message via producer, check kafka logs %s" % e)
                return constants.KAFKA_MESSAGE_NOT_SEND_PRODUCER

    return constants.ALL_OK
