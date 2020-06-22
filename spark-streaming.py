from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from pyspark.streaming.kafka import KafkaUtils


# consumer = KafkaConsumer('politics-kafka', bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',
#      enable_auto_commit=True,
#      group_id='my-group',
#      value_deserializer=lambda x: loads(x.decode('utf-8')))

sc = SparkContext(appName="PythonSparkStreamingKafka")
sc.setLogLevel("WARN")

streaming_context = StreamingContext(sc,10)

kafkaStream = KafkaUtils.createStream(streaming_context, 'localhost:9092', 'spark-streaming', {'politics-kafka':1})


