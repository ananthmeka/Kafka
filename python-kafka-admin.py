import logging
import os
import time
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource
from kafka.errors import IncompatibleBrokerVersion, KafkaConfigurationError, UnrecognizedBrokerVersion


class KafkaAdmin:
        # Instantiate the AdminClient
        self.admin_client = KafkaAdminClient(bootstrap_servers=os.getenv("KAFKA_BROKER_DNS_NAME") + ":" + "9092",
                                             client_id='kafkaadmin',
                                             security_protocol="SSL",
                                             ssl_certfile="<SSL CERT FILE PATH>",
                                             ssl_cafile="<SSL CA FILE PATH>",
                                             ssl_keyfile="<SSL KEY FILE PATH>",
                                             ssl_check_hostname=False)
    def describe_cluster(self):
        # describe the cluster
        return self.admin_client.describe_cluster()

    def list_topics(self):
        # list all topics
        return self.admin_client.list_topics()

    def is_topic_exists(self, topic):
        # returns True , if the topic already exists
        if topic in self.list_topics():
            return True
        return False

    def describe_topics(self, topics):
        # describe the topics 
        if not isinstance(topics, list):
            print("describe_topics() function expects topics as list")
            return []
        return self.admin_client.describe_topics(topics=topics)

    def alter_topic_config(self, topic, topic_configs):
        # alter the topic config settings

        if not self.is_topic_exists(topic):
            print("Topic [%s] not Exists for alter_topic_cofig" %(topic))
            return False
        if topic_configs:
            config_resource = ConfigResource(resource_type='TOPIC', name=topic, configs=topic_configs)
            self.admin_client.alter_configs([config_resource])
            print("Topic [%s] alterered" %(topic))
        else:
            print("Topic config for [%s] is Not alterered" %(topic))
        return True
            
    def create_topic(self, topic, num_partitions=1, replication_factor=3, topic_configs=None):
        """ Creates the kafka topic """

        # check if the topic already exist, if so do nothing for now (return error later)
        if self.is_topic_exists(topic):
            print("Topic [%s] Already Exists" %(topic))
            return False
        #create the topic
        topic_list = []
        topic_list.append(NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor, topic_configs=topic_configs))
        try:
            self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except (NotImplementedError, KafkaConfigurationError, IncompatibleBrokerVersion, UnrecognizedBrokerVersion, RuntimeError) as err:
            print("Topic [%s] Not created : %s" (topic, str(err)))
            return False
        print("Topic [%s] created" %(topic))
        return True

    def delete_topic(self, topic):
        # deletes the existing topic

        if self.is_topic_exists(topic):
            self.admin_client.delete_topics([topic], 1000)
            print("Topic [%s] is deleted" %(topic))
        else:
            print("Topic [%s] does not exists for deletion" %(topic))
            return False
        return True

if __name__ == '__main__':
    admin = KafkaAdmin()
    admin.delete_topic('mytest')
    time.sleep(2)
    admin.create_topic('mytest')
    admin.alter_topic_config('mytest', {"retention.ms":"600000"})
    print(admin.describe_topics(['mytest']))
    print(admin.describe_cluster())
