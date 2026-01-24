import json
import pandas as pd
import numpy as np
from loguru import logger
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.avro import AvroProducer


def load_schema(schema_path: str) -> str:
    """Load avro schema from file

    :param str schema_path: Path to file with avro schema
    :return str: Avro schema string
    """
    logger.debug(f'Load avro schema by path {schema_path}')
    with open(f"{schema_path}") as f:
        schema_str = f.read()
    return schema_str

def delivery_report(err, msg):
    """Report with produce data result

    :param err: Error description
    :param msg: Message description
    """
    if err is not None:
        print("Delivery failed for record: {}".format(err))
        return
    print('Record successfully produced to {} [{}] at offset {}'.format(
        msg.topic(), msg.partition(), msg.offset()))
    
def get_msgs_from_csv(csv_path:str) -> list[dict]:
    """Read csv file and transform it 
    into list on messages for kafka

    :param str csv_path: Path to csv file with data
    :return list[dict]: List of messages for kafka
    """
    logger.debug(f'Read file by path {csv_path}')
    df = pd.read_csv(csv_path)
    # df = df.dropna()
    # df = df.astype({'PUlocationID': 'Int64',
    #                 'DOlocationID': 'Int64',
    #                 'SR_Flag': 'Int64'})
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.head(15)

    return df.to_dict(orient='records')

def produce_messages_json(topic:str, 
                          messages:list[dict], 
                          conf:dict,
                          delivery_report:callable) -> None:
    """Produce messages to kafka topic

    :param str topic: Kafka topic name
    :param list[dict] Messages: List of messages to send
    :param dict conf: Configuration of kafka produser
    :param function delivery_report: Delivery report
    """
    logger.debug('Produce messages')
    # value_serializer = AvroSerializer(schema_registry_client, schema_value)
    kafka_produser = Producer(conf)
    def produce_flush(msg):
        kafka_produser.produce(topic=topic,
                               value=f"{json.dumps(msg)}".encode("utf-8"),
                               on_delivery=delivery_report)
        kafka_produser.poll(0)
    [produce_flush(msg) for msg in messages]
    kafka_produser.flush()

def produce_messages_avro(topic:str, 
                          messages:list[dict], 
                          schema_value:str, 
                          conf:dict,
                        #   delivery_report:callable
                          ) -> None:
    """Produce messages to kafka topic

    :param str topic: Kafka topic name
    :param list[dict] Messages: List of messages to send
    :param str schema_value: Avro schema of message value
    :param _type_ schema_registry_client: Schema registry conf
    :param dict conf: Configuration of kafka produser
    :param function delivery_report: Delivery report
    """
    logger.debug('Produce messages')
    kafka_produser = AvroProducer(conf, default_value_schema=schema_value)
    def produce_flush(msg):
        kafka_produser.produce(topic=topic,
                               value=msg)
        kafka_produser.poll(0)
    [produce_flush(msg) for msg in messages]
    kafka_produser.flush()