#!/usr/bin/env python
import pika
import sys

exchange_name = 'topic_logs'


def producer_connection_init():
    """
        Initialize the connection to RabbitMQ and return the channel and the connection.
    """
    
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)

    return channel, connection

def publish_message(channel, topic: str, body: str):
    channel.basic_publish(
    exchange=exchange_name, routing_key=topic, body=body)

def connection_end(connection, channel):
    """
        Close the connection to RabbitMQ.
    """
    channel.close()
    connection.close()

def listen(topic: str, callback):
    """
        :param str topic: The topic from which to consume
        :param callable on_message_callback: Required function to handle incoming messages, having the signature:
            on_message_callback(channel, method, properties, body)
            - channel: BlockingChannel
            - method: spec.Basic.Deliver
            - properties: spec.BasicProperties
            - body: bytes
    """
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)

    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(
        exchange=exchange_name, queue=queue_name, routing_key=topic)

    print(' [*] Waiting for logs. To exit press CTRL+C')

    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()
    
