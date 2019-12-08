# -*- coding:utf-8 -*-

import os
from datetime import datetime
from dotenv import load_dotenv
from amqplib import client_0_8 as amqp


class Publisher(object):

    def __init__(self):
        load_dotenv()
        self.rabbitMQConf = {"hostPort": os.getenv('RABBITMQ_HOST') + ':' + os.getenv('RABBITMQ_PORT'),
                             "user": os.getenv('RABBITMQ_USER'),
                             "pass": os.getenv('RABBITMQ_PASS'),
                             "virtualHost": os.getenv('RABBITMQ_VHOST')
                             }
        self.queueConf = {"exchange": os.getenv('RABBITMQ_QUEUE_EXCHANGE')}

        # print(self.rabbitMQConf)
        # print(self.queueConf)
        # exit(0)

    def send_job(self, job_id):

        try:
            conn = amqp.Connection(self.rabbitMQConf['hostPort'],
                                   self.rabbitMQConf['user'],
                                   self.rabbitMQConf['pass'],
                                   virtualHost=self.rabbitMQConf['virtualHost'],
                                   insist=False)

            chan = conn.channel()
            chan.access_request(self.rabbitMQConf['virtualHost'])

            new_message = amqp.Message(job_id)
            chan.basic_publish(new_message, exchange=self.queueConf['exchange'])

            chan.close()
            conn.close()

            # $msg = new AMQPMessage($userId, array('content_type' = > 'text/plain'));
            # $channel->basic_publish($msg, $this->queueConf['exchange']);
            # $channel->close();
            # $conn->close();

            return True

        except Exception as exc:
            print("send_Exception")
            print(exc)


if __name__ == '__main__':
    mailQueue = Publisher()
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " sending job: ")
    mailQueue.send_job("nebula " + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
