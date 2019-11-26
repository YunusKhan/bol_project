# from pykafka import rdkafka
from pykafka import KafkaClient
import logging
import constants


def get_kafka_client():
    # try catch here

    """
    Get a new or existing kafka client

    :return: kafka client
    """

    try:
        return KafkaClient(hosts=constants.kafka_hostname_port)
    except ValueError as e:
        logging.debug("Unable to get Kafka client, check if brokers "
                      "are too less needs to be and a new one needs "
                      "to be created, %s" % e)


def set_kafka_client():
    """
    Set a new kafka client usually in case the earlier
    one is not available by using a new broker

    :return: kafka client
    """

    logging.info("Implement setting the kafka client")
    try:
        return KafkaClient(hosts=constants.kafka_hostname_port)
    except ValueError as e:
        logging.debug("Unable to get Kafka client, check if brokers "
                      "are too less needs to be and a new one needs "
                      "to be created, %s" % e)


def get_kafka_topic(kafka_client_name, kafka_topic_name):
    """
    Get the kafka topic
    :param : kafka_client_name : kafka client
    :param : kafka_topic_name : kafka topic
    :return: kafka topic
    """

    logging.info("Implement getting the kafka topic")
    try:
        return kafka_client_name.topics[kafka_topic_name]
    except ValueError as e:
        logging.debug("Unable to get Kafka topic, check if brokers "
                      "are too less needs to be and a new one needs "
                      "to be created, %s" % e)


def set_kafka_topic(kafka_client_name, kafka_topic_name):
    """
    Set a new kafka topic usually in case the earlier
    one is not available by using a new broker
    :param : kafka_client_name : kafka client
    :param : kafka_topic_name : kafka topic
    :return: kafka topic
    """
    logging.info("Implement setting the kafka topic")
    try:
        return kafka_client_name.topics[kafka_topic_name]
    except ValueError as e:
        logging.debug("Unable to get Kafka topic, check if brokers "
                      "are too less needs to be and a new one needs "
                      "to be created, %s" % e)
