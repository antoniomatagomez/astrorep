import time
import logging as log

from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from vs_fmc_plugin.hooks.kafka_hook import KafkaHook
from vs_fmc_plugin.operators.db_table import DBTable


class KafkaToJdbcTransfer(BaseOperator):
    """
    Move data from Kafka to JDBC target source in batches.

    :param src_mtd: dictionary containing the metadata of the Kafka source
    :param target_mtd: dictionary containing the metadata of the target table
    :param src_conn_id: reference to the source Kafka connections
    :param target_conn_id: reference to the source database connection
    :param batch_size: maximum number or records loaded at the same time (str containing an int or int)

    """

    @apply_defaults
    def __init__(self, src_mtd, target_mtd, src_conn_id, target_conn_id, batch_size, *args, **kwargs):
        self.src_mtd = src_mtd
        self.target_mtd = target_mtd
        self.src_conn_id = src_conn_id
        self.target_conn_id = target_conn_id
        self.batch_size = int(batch_size)

        super(KafkaToJdbcTransfer, self).__init__(*args, **kwargs)

    def execute(self, context):
        from airflow.providers.jdbc.hooks.jdbc import JdbcHook

        self.log.info("Establishing Kafka to JDBC operator...")

        target_hook = JdbcHook(jdbc_conn_id=self.target_conn_id)
        target_conn = target_hook.get_conn()
        target_hook.set_autocommit(target_conn, False)
        target_cur = target_conn.cursor()

        self.log.info("Connected to target database.")

        src_hook = KafkaHook(kafka_conn_id=self.src_conn_id)
        max_number_of_messages = src_hook.max_number_of_messages
        message_polling_timeout = src_hook.message_polling_timeout

        kafka_topic = self.src_mtd['src_kafka_topic']

        self.log.info(f"Received Kafka topic from source metadata: {kafka_topic}")

        consumer_config = src_hook.get_consumer_config()

        self.consume_kafka_messages(
            kafka_topic,
            consumer_config,
            self.batch_size,
            target_conn,
            target_cur,
            max_number_of_messages,
            message_polling_timeout
        )

        target_cur.close()
        target_conn.close()

    def consume_kafka_messages(self, kafka_topic, consumer_config, batch_size, target_conn, target_cur,
                               max_number_of_messages, message_polling_timeout):
        from confluent_kafka.avro import AvroConsumer
        from confluent_kafka.avro.serializer import SerializerError
        from confluent_kafka import TopicPartition

        def onAvroConsumerError(err):
            raise Exception(err)

        consumer_config['error_cb'] = onAvroConsumerError

        consumer = AvroConsumer(consumer_config)

        kafka_topic_count = 1
        current_timestamp = time.time() * 1000

        print(f"Consuming Kafka topic: {kafka_topic}")

        consumer.subscribe([kafka_topic])

        msg = None
        count = 1
        raw_batch = []
        batch_count = 1
        offsets_to_commit = []

        while True:

            try:

                msg = consumer.poll(message_polling_timeout)

            except SerializerError as e:
                print(f"Message deserialization failed for {msg}: {e}")
                raw_batch = []
                break

            if msg is None:
                break

            if msg.error():
                print(f"AvroConsumer error: {msg.error()}")
                break

            if msg.timestamp()[1] > current_timestamp:
                break

            raw_batch.append({"value": msg.value(), "timestamp": msg.timestamp()[1], "offset": msg.offset()})

            offset_to_commit_metadata = TopicPartition(kafka_topic, msg.partition(), msg.offset() + 1)
            offsets_to_commit.append(offset_to_commit_metadata)

            if not count % batch_size:
                print(f"- got batch number {batch_count} containing {batch_size} message(s)")

                data_batch = self.prepare_data_batch(raw_batch, kafka_topic)
                self.send_data_batch_to_db(data_batch, consumer, offsets_to_commit, target_conn, target_cur)

                batch_count += 1
                raw_batch = []
                offsets_to_commit = []

            if max_number_of_messages != -1 and count >= max_number_of_messages:
                print(f"Reached a limit of {max_number_of_messages} consumable message(s)")
                break

            count += 1

        print("Done consuming Kafka topic:", kafka_topic)

        if len(raw_batch) > 0:
            print(f"- received a batch of {len(raw_batch)} messages")
            data_batch = self.prepare_data_batch(raw_batch, kafka_topic)
            self.send_data_batch_to_db(data_batch, consumer, offsets_to_commit, target_conn, target_cur)

        consumer.unsubscribe()
        kafka_topic_count += 1

        consumer.close()

        print("Kafka to JDBC operations finished.")

        return

    def prepare_data_batch(self, raw_batch, kafka_topic):

        prepared_data_batch = {}

        for message_data in raw_batch:
            message = message_data["value"]
            timestamp = message_data["timestamp"]
            offset = message_data["offset"]
            prepared_data_batch = self.parse_kafka_message(prepared_data_batch, message, timestamp, offset, kafka_topic)

        return prepared_data_batch

    def parse_kafka_message(self, prepared_data_batch, message, timestamp, offset, kafka_topic):
        import jpype

        table_name = self.clean_string(kafka_topic)

        target_table = next((item for item in self.target_mtd['target_table_names'] if item["table_name_src"] == table_name),
                            None)

        if table_name not in prepared_data_batch:
            prepared_data_batch[table_name] = {
                "column_names": None,
                "column_values": []
            }

        column_names = []
        column_values = []

        for key, value in message.items():

            if type(value) == dict:
                nested_table_name = table_name + "_" + self.clean_string(key)
                prepared_data_batch = self.parse_kafka_message(prepared_data_batch, value, timestamp, offset, nested_table_name)
            elif target_table is not None and key in target_table["column_names"]:
                column_names.append(key)
                column_values.append(value)

        if target_table is not None:

            if "timestamp_column" in target_table:
                column_names.append(target_table["timestamp_column"])
                utc_timestamp_ms = int(datetime.utcfromtimestamp(timestamp / 1000).timestamp() * 1000)
                sql_timestamp = jpype.java.sql.Timestamp(utc_timestamp_ms)
                column_values.append(sql_timestamp)
            if "offset_column" in target_table:
                column_names.append(target_table["offset_column"])
                column_values.append(offset)

            prepared_data_batch[table_name]["column_names"] = column_names
            prepared_data_batch[table_name]["column_values"].append(tuple(column_values))

        return prepared_data_batch

    @staticmethod
    def clean_string(string_input):

        string_input = string_input.replace(".", "_")
        string_input = string_input.replace("-", "_")

        return string_input

    def send_data_batch_to_db(self, data_batch, consumer, offsets_to_commit, target_conn, target_cur):

        try:

            for table_name, table_props in data_batch.items():

                target_table = next(
                    (item for item in self.target_mtd['target_table_names'] if item["table_name_src"] == table_name), None)

                if target_table is not None:

                    target_mtd = {
                        "schema_name": self.target_mtd["schema_name"],
                        "table_name": target_table['table_name'],
                        "column_names": table_props['column_names']
                    }
                    target_table = DBTable(target_mtd)
                    insert_query = target_table.insert_query()
                    target_cur.executemany(insert_query, table_props["column_values"])

                else:
                    print(
                        f"Skipping table '{table_name}' since it is not provided in the source metadata target_table_names list")

            target_conn.commit()
            print(f"Done inserting data batch into the table(s)")
            self.commit_offsets(consumer, offsets_to_commit)

        except Exception as e:
            err_msg = f"Error while sending data to target source': \n {e}"
            raise Exception(err_msg)

        return

    @staticmethod
    def commit_offsets(consumer, offsets_to_commit):

        try:
            for offset_to_commit in offsets_to_commit:
                consumer.commit(offsets=[offset_to_commit], asynchronous=False)
            print("Done committing offsets")
        except Exception as e:
            log.error("Error committing offsets")
            log.error(e)
