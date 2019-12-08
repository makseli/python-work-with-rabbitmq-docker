# -*- coding:utf-8 -*-

import os
from datetime import datetime
from dotenv import load_dotenv
from amqplib import client_0_8 as amqp


class Consumer(object):

    def __init__(self):
        load_dotenv()
        self.rabbitMQConf = {"hostPort": os.getenv('RABBITMQ_HOST') + ':' + os.getenv('RABBITMQ_PORT'),
                             "user": os.getenv('RABBITMQ_USER'),
                             "pass": os.getenv('RABBITMQ_PASS'),
                             "virtualHost": os.getenv('RABBITMQ_VHOST')
                             }
        self.queueConf = {"name": os.getenv('RABBITMQ_QUEUE_NAME'),
                          "exchange": os.getenv('RABBITMQ_QUEUE_EXCHANGE')
                          }

        # print(self.rabbitMQConf)
        # print(self.queueConf)
        # exit(0)

    def listen(self):

        try:
            conn = amqp.Connection(self.rabbitMQConf['hostPort'],
                                   self.rabbitMQConf['user'],
                                   self.rabbitMQConf['pass'],
                                   virtualHost=self.rabbitMQConf['virtualHost'],
                                   insist=False)

            chan = conn.channel()
            chan.queue_declare(self.queueConf['name'],
                               durable=True,
                               exclusive=False,
                               auto_delete=False)
            chan.exchange_declare(self.queueConf['exchange'],
                                  type="direct",
                                  durable=True,
                                  auto_delete=False)

            chan.queue_bind(self.queueConf['name'], self.queueConf['exchange'])

            chan.basic_consume(self.queueConf['name'],
                               no_ack=True,
                               callback=self.do_jobs)
            # start listen
            print("starting listen; ")
            while True:
                chan.wait()

        except Exception as exc:
            print("listen_Exception")
            print(exc)

    # noinspection PyMethodMayBeStatic
    def do_jobs(self, msg):
        print(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " new job: ")
        get_value_from_queue = msg.body
        print("get_value_from_queue : " + get_value_from_queue)


if __name__ == '__main__':
    mailQueue = Consumer()
    mailQueue.listen()
