# Databricks notebook source
# MAGIC %md
# MAGIC ###DimUser
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


import os
import sys



projectpath=os.path.join(os.getcwd(),'..','..')
sys.path.append(projectpath)





# COMMAND ----------

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ###Autoloader

# COMMAND ----------

dim_user=spark.readStream.format("cloudFiles")\
.option("cloudFiles.format","parquet")\
.option("cloudFiles.schemaLocation","abfss://silver@sttacntspotify.dfs.core.windows.net/DimUser/checkpoint")\
.option("cloudFiles.schemaEvolutionMode","addNewColumns")\
.load("abfss://bronze@sttacntspotify.dfs.core.windows.net/DimUser")


# COMMAND ----------

display(dim_user)

# COMMAND ----------

df_user_obj=reusable()
df_user=df_user_obj.dropColumns(dim_user,['_rescued_date'])
df_user=dim_user.withColumn("user_name",upper(col("user_name")))
df_user=df_user.dropDuplicates(["user_id"])
display(df_user)





# COMMAND ----------

for s in spark.streams.active:
    print("Stopping:", s.name, s.id)
    s.stop()

# COMMAND ----------

dim_user.writeStream\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@sttacntspotify.dfs.core.windows.net/DimUser/ checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@sttacntspotify.dfs.core.windows.net/DimUser/data")\
    .toTable("mydeltalakecatalog.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimArtist

# COMMAND ----------

dim_artist=spark.readStream.format("cloudFiles")\
.option("cloudFiles.format","parquet")\
.option("cloudFiles.schemaLocation","abfss://silver@sttacntspotify.dfs.core.windows.net/DimArtist/checkpoint")\
.option("cloudFiles.schemaEvolutionMode","addNewColumns")\
.load("abfss://bronze@sttacntspotify.dfs.core.windows.net/DimArtist")

# COMMAND ----------

display(dim_artist)

# COMMAND ----------

df_art_obj=reusable()
dim_artist=df_art_obj.dropColumns(dim_artist,['_rescued_date'])
df_artist=dim_artist.dropDuplicates(["artist_id"])
display(dim_artist)

# COMMAND ----------

dim_artist.writeStream\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@sttacntspotify.dfs.core.windows.net/DimArtist/ checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@sttacntspotify.dfs.core.windows.net/DimArtist/data")\
    .toTable("mydeltalakecatalog.silver.DimArtist")
        

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists mydeltalakecatalog.silver

# COMMAND ----------

dim_track=spark.readStream.format("cloudFiles")\
.option("cloudFiles.format","parquet")\
.option("cloudFiles.schemaLocation","abfss://silver@sttacntspotify.dfs.core.windows.net/DimTrack/checkpoint")\
.option("cloudFiles.schemaEvolutionMode","addNewColumns")\
.load("abfss://bronze@sttacntspotify.dfs.core.windows.net/DimTrack")


# COMMAND ----------

dim_track = (
    dim_track
        .withColumn(
            "durationflag",
            when(col("duration_sec") < 150, "low")
            .when(col("duration_sec") < 300, "medium")
            .otherwise("very high")
        )
)

dim_track = dim_track.withColumn("track_name", regexp_replace(col("track_name"), '-', ''))

df_track_obj=reusable()
dim_track=df_track_obj.dropColumns(dim_track,['_rescued_date'])
display(dim_track)


# COMMAND ----------

dim_track.writeStream\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@sttacntspotify.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@sttacntspotify.dfs.core.windows.net/DimTrack/data")\
    .toTable("mydeltalakecatalog.silver.DimTrack")


# COMMAND ----------

dim_date=spark.readStream.format("cloudFiles")\
.option("cloudFiles.format","parquet")\
.option("cloudFiles.schemaLocation","abfss://silver@sttacntspotify.dfs.core.windows.net/DimDate/checkpoint")\
.option("cloudFiles.schemaEvolutionMode","addNewColumns")\
.load("abfss://bronze@sttacntspotify.dfs.core.windows.net/DimDate")


# COMMAND ----------

dim_dat_obj=reusable()
dim_date=dim_dat_obj.dropColumns(dim_date,['_rescued_date'])

dim_date.writeStream\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@sttacntspotify.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@sttacntspotify.dfs.core.windows.net/DimDate/data")\
    .toTable("mydeltalakecatalog.silver.DimDate")



# COMMAND ----------

# MAGIC %md
# MAGIC ###FactStream
# MAGIC

# COMMAND ----------

fact_stream=spark.readStream.format("cloudFiles")\
.option("cloudFiles.format","parquet")\
.option("cloudFiles.schemaLocation","abfss://silver@sttacntspotify.dfs.core.windows.net/FactStream/checkpoint")\
.option("cloudFiles.schemaEvolutionMode","addNewColumns")\
.load("abfss://bronze@sttacntspotify.dfs.core.windows.net/FactStream")

# COMMAND ----------

fact_str_obj=reusable()
fact_stream=fact_str_obj.dropColumns(fact_stream,['_rescued_date'])

fact_stream.writeStream\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@sttacntspotify.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@sttacntspotify.dfs.core.windows.net/FactStream/data")\
    .toTable("mydeltalakecatalog.silver.FactStream")



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE mydeltalakecatalog.silver.dimartist SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC ALTER TABLE mydeltalakecatalog.silver.dimartist
# MAGIC DROP COLUMNS (_rescued_data);

# COMMAND ----------

