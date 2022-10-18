# Databricks notebook source
# key and secrete for connect kafka topics
kafkaUser="O5NENUFKK4SIVUA5"
kafkaSecret = "2J7VIscdCIEfRIcyLUE15nplkkbZTE69533j6d9acByoUEcFQ2UTc6bk7U0shgp0"

# COMMAND ----------

# MAGIC %md
# MAGIC ### load streaming data flow from kafka topic clickstream, and do some streaming data transform.

# COMMAND ----------

#connect kafka topic clickstream, this topic include the information for user web click information
( 
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "pkc-3w22w.us-central1.gcp.confluent.cloud:9092")
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(kafkaUser, kafkaSecret))
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", "clickstream")
  .option("startingOffsets", "earliest")
  .load()
  .createOrReplaceTempView("click_stream")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from click_stream

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW click_stream_parsed
# MAGIC AS
# MAGIC   SELECT string(key), string(value),topic, partition, offset, timestamp, timestampType
# MAGIC   FROM click_stream;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW click_stream_final
# MAGIC AS
# MAGIC   SELECT key, value:_time, cast(value:time as bigint), value:ip, value:request, cast(value:status as int), value:userid, cast(value:bytes as bigint), value:agent, topic, partition, offset, timestamp, timestampType
# MAGIC   FROM click_stream_parsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from click_stream_final

# COMMAND ----------

from pyspark.sql.functions import *

df_click_stream = (
spark.table('click_stream_final'))




# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from click_stream_final

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### -- Table of html pages per minute for each user, create a new table: pages_per_min, and load it to GCS, slide window

# COMMAND ----------

from pyspark.sql.functions import *

(df_click_stream
.withWatermark("timestamp", "60 seconds")
.filter("request LIKE '%html%'")
.groupBy(window(col("timestamp"), "60 seconds", "5 second"), "userid")
.agg(count("*").alias("pages"))
.select(col("userid"),col("window.start").alias("EVENT_start"), col("window.end").alias("EVENT_end"), col("pages"))
.createOrReplaceTempView("click_stream_analyse"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from click_stream_analyse
# MAGIC where userid = 19
# MAGIC order by event_start desc

# COMMAND ----------

query = (spark
.readStream
.table("click_stream_analyse")
.writeStream
.option("checkpointLocation", "/FileStore/tables/checkpoint/pages_per_min")
.outputMode("append")
.trigger(processingTime='4 seconds')
.table("pages_per_min"))



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pages_per_min

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail pages_per_min

# COMMAND ----------

# MAGIC %md
# MAGIC ### -- number of errors per min, Filter to show ERROR codes > 400
# MAGIC ### -- where count > 5
# MAGIC ### -- slide window

# COMMAND ----------

(df_click_stream
.withWatermark("timestamp", "60 seconds")
.filter("status > 400")
.groupBy(window(col("timestamp"), "60 seconds", "20 seconds"), col("status"))
.agg(count("*").alias("errors"))
.filter("errors > 5 AND errors IS NOT NULL")
.select(col("status"),col("window.start").alias("EVENT_start"), col("window.end").alias("EVENT_end"), col("errors"))
.createOrReplaceTempView("click_stream_error"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from click_stream_error

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from click_stream_error
# MAGIC where status = 404
# MAGIC order by event_start desc

# COMMAND ----------

query = (spark
.readStream
.table("click_stream_error")
.writeStream
.option("checkpointLocation", "/FileStore/tables/checkpoint/errors_per_min_alert")
.outputMode("append")
.trigger(processingTime='4 seconds')
.table("errors_per_min_alert"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### load streaming data flow from kafka topic: pksqlc-n0r06USER_CLICKSTREAM, and do some streaming data transform.
# MAGIC ### This topic contain both user and clickstream information

# COMMAND ----------

#connect kafka topic pksqlc-n0r06USER_CLICKSTREAM, this topic include the information for user web click information and user information
( 
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "pkc-3w22w.us-central1.gcp.confluent.cloud:9092")
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(kafkaUser, kafkaSecret))
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", "pksqlc-n0r06USER_CLICKSTREAM")
  .option("startingOffsets", "earliest")
  .load()
  .createOrReplaceTempView("user_clicks")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from user_clicks

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_clicks

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_click_string
# MAGIC AS
# MAGIC   SELECT string(key), string(value),topic, partition, offset, timestamp, timestampType
# MAGIC   FROM user_clicks;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_click_string

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_user_click AS
# MAGIC   SELECT key, from_json(value, schema_of_json('{"USERNAME":"ArlyneW8ter","REGISTERED_AT":1420654807513,"IP":"222.245.174.222","CITY":"London","REQUEST":"GET /index.html HTTP/1.1","STATUS":404,"BYTES":4006}')) AS value, topic, partition, offset, timestamp, timestampType
# MAGIC   FROM user_click_string;
# MAGIC   
# MAGIC SELECT * FROM parsed_user_click

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW user_click_final AS
# MAGIC   SELECT key, value.*, topic, partition, offset, timestamp, timestampType
# MAGIC   FROM parsed_user_click;
# MAGIC   
# MAGIC SELECT * FROM user_click_final;

# COMMAND ----------

# MAGIC %md
# MAGIC ### -- User sessions table - 30 seconds of inactivity expires the session
# MAGIC ### -- Table counts number of events within the session

# COMMAND ----------

(spark
.table("user_click_final")
.withWatermark("timestamp", "30 seconds")
.groupBy(session_window(col("timestamp"), "30 seconds"), col("USERNAME"))
.agg(count("*").alias("events"))
.select(col("USERNAME"),col("session_window.start").alias("EVENT_start"), col("session_window.end").alias("EVENT_end"), col("events"))
.createOrReplaceTempView("click_user_session_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from click_user_session_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from click_user_session_temp
# MAGIC where username = 'Nathan_126' 
# MAGIC order by event_start desc

# COMMAND ----------

query = (spark
.readStream
.table("click_user_session_temp")
.writeStream
.option("checkpointLocation", "/FileStore/tables/checkpoint/click_user_sessions")
.outputMode("append")
.trigger(processingTime='4 seconds')
.table("click_user_sessions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### -- Enriched user details table:
# MAGIC ### -- Aggregate (count&groupBy) using a TUMBLING-Window
# MAGIC ### -- count user activity within 60s

# COMMAND ----------

(spark
.table("user_click_final")
.withWatermark("timestamp", "60 seconds")
.groupBy(window(col("timestamp"), "60 seconds"), col("username"),col("ip"), col("city"))
.agg(count("*").alias("count"))
.filter("count > 1")
.select(col("username"),col("ip"), col("city"), col("window.start").alias("EVENT_start"),col("window.end").alias("EVENT_end"), col("count"))
.createOrReplaceTempView("user_ip_activity_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_ip_activity_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_ip_activity_temp
# MAGIC where username = "Reeva43" and ip = "111.245.174.111" and city = "Raleigh"
# MAGIC order by event_start desc

# COMMAND ----------

query = (spark
.readStream
.table("user_ip_activity_temp")
.writeStream
.option("checkpointLocation", "/FileStore/tables/checkpoint/user_ip_activity")
.outputMode("append")
.trigger(processingTime='4 seconds')
.table("user_ip_activity"))
