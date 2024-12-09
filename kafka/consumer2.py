import logging
import os
import tempfile
import datetime
from logging.handlers import RotatingFileHandler
from kafka import KafkaConsumer
from hdfs import InsecureClient
import json


class KafkaToHDFSConsumer:
    def __init__(self, topic, hdfs_url, hdfs_user, group_id, brokers):
        # Cấu hình logger
        log_handler = RotatingFileHandler(
            f"{os.path.abspath(os.getcwd())}/kafka/logs/consumer.log",
            maxBytes=104857600, backupCount=10)
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG,
            handlers=[log_handler])
        self.logger = logging.getLogger('consumer')

        # Cấu hình Kafka Consumer
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Cấu hình HDFS client
        self.logger.debug(f"HDFS client is using URL: {hdfs_url}")
        self.hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)

    def flush_to_hdfs(self, tmp_file_name):
        # Tạo tên file HDFS dựa trên thời gian hiện tại
        current_time = datetime.datetime.now()
        hdfs_filename = f"/stockData/{current_time.year}/{current_time.month}/{current_time.day}/stockData.{int(current_time.timestamp())}.json"

        self.logger.info(f"Starting flush file {tmp_file_name} to HDFS...")
        try:
            # Log chi tiết URL và tên file
            self.logger.debug(f"Uploading to HDFS URL: {self.hdfs_client.url}{hdfs_filename}")
            
            # Upload file
            self.hdfs_client.upload(hdfs_filename, tmp_file_name)
            
            self.logger.info(f"Successfully uploaded {tmp_file_name} to HDFS as {hdfs_filename}.")
            self.consumer.commit()  # Commit offset sau khi flush thành công
        except Exception as e:
            self.logger.error(f"Failed to upload {tmp_file_name} to HDFS: {e}")
            raise e

    def recreate_tmpfile(self):
        # Tạo file tạm để lưu dữ liệu trước khi upload
        tmp_file = tempfile.NamedTemporaryFile(mode='w+t', delete=False)
        return tmp_file

    def run(self):
        try:
            tmp_file = self.recreate_tmpfile()
            self.logger.info("Subscribed to Kafka topic and started consuming messages.")
            while True:
                # Poll messages từ Kafka
                self.logger.debug("Polling messages from Kafka...")
                msgs_pack = self.consumer.poll(10.0)
                if not msgs_pack:
                    self.logger.info("No messages received in the last 10 seconds.")
                    continue

                for tp, messages in msgs_pack.items():
                    self.logger.info(f"Received {len(messages)} messages from partition {tp.partition}")
                    for message in messages:
                        try:
                            # Log nội dung message
                            self.logger.debug(f"Received message: {message.value}")
                            tmp_file.write(json.dumps(message.value) + "\n")  # Ghi dữ liệu vào file tạm
                            self.logger.debug(f"Message written to temp file: {tmp_file.name}")
                            print(f"Consumed message: {message.value}")
                        except Exception as e:
                            self.logger.error(f"Error processing message: {e}")
                            continue
                # Nếu file tạm vượt quá kích thước, thì flush vào HDFS
                if tmp_file.tell() > 10 :
                    self.flush_to_hdfs(tmp_file.name)
                    tmp_file.close()
                    tmp_file = self.recreate_tmpfile()

        except Exception as e:
            self.logger.error(f"An error occurred in the consumer loop: {e}")
        finally:
            try:
                tmp_file.close()
                self.logger.info(f"Temp file {tmp_file.name} closed.")
            except Exception as e:
                self.logger.error(f"Error closing temp file: {e}")
            try:
                self.consumer.close()
                self.logger.info("Kafka consumer closed.")
            except Exception as e:
                self.logger.error(f"Error closing Kafka consumer: {e}")


if __name__ == "__main__":
    # Cấu hình thông số
    topic = "example_topic"
    hdfs_url = "http://localhost:9870"
    hdfs_user = "root"
    group_id = "group1"
    brokers = ["localhost:9092", "localhost:9093", "localhost:9094"]

    # Khởi chạy consumer
    consumer = KafkaToHDFSConsumer(topic, hdfs_url, hdfs_user, group_id, brokers)
    consumer.run()
