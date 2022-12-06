// References:
// https://medium.com/@awabmohammedomer/how-to-fit-a-linear-regression-model-in-apache-spark-using-scala-f246dd06a3b1

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, OneHotEncoderEstimator}
import org.apache.spark.ml.regression.{RandomForestRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
import sqlContext.implicits._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions.{to_timestamp}

var df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("project/clean/output/clean.csv")

df = df.withColumnRenamed("_c2", "inBed")
df = df.withColumnRenamed("_c6", "efficiency")
df = df.withColumn("inBed", (col("inBed")))
df = df.withColumn("efficiency", col("efficiency").cast("double"))
df = df.select(col("*"), substring(col("inBed"), 0, 2).as("inBedHours"))
df = df.withColumn("inBedHours", col("inBedHours").cast("double"))

val efficiency = df.select($"efficiency").rdd.map(_.getDouble(0))
val inBedHours = df.select($"inBedHours").rdd.map(_.getDouble(0))

val correlation: Double = Statistics.corr(efficiency, inBedHours, "spearman")

print(correlation)



df = df.withColumnRenamed("_c4", "asleep")
df = df.withColumn("asleep", (col("asleep")))
df = df.select(col("*"), substring(col("asleep"), 0, 2).as("asleepHours"))
df = df.withColumn("asleepHours", col("asleepHours").cast("double"))

val asleepHours = df.select($"asleepHours").rdd.map(_.getDouble(0))

val correlation2: Double = Statistics.corr(asleepHours, inBedHours, "spearman")

print(correlation2)


df = df.withColumnRenamed("_c3", "fellAsleepIn")
df = df.withColumnRenamed("_c10", "sleepBPM")

df = df.withColumn("fellAsleepIn", (col("fellAsleepIn")))
df = df.withColumn("sleepBPM", (col("sleepBPM")))

df = df.withColumn("sleepBPM", col("sleepBPM").cast("double"))
df = df.select(col("*"), substring(col("fellAsleepIn"), 0, 2).as("fellAsleepInHours"))
df = df.withColumn("fellAsleepInHours", col("fellAsleepInHours").cast("double"))

val fellAsleepInHours = df.select($"fellAsleepInHours").rdd.map(_.getDouble(0))
val sleepBPM = df.select($"sleepBPM").rdd.map(_.getDouble(0))

val correlation3: Double = Statistics.corr(fellAsleepInHours, sleepBPM, "spearman")

print(correlation3)
// print(df.show(4))