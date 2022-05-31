package code;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.*;

class KafkaTask {
    public static void main(final String[] args) {
        SparkSession spark = SparkSession
                                .builder()
                                .master("yarn")
                                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.3.68.20:9092,"
                        + " 10.3.68.21:9092, 10.3.68.23:9092,"
                        + " 10.3.68.26:9092, 10.3.68.28:9092,"
                        + " 10.3.68.32:9092, 10.3.68.47:9092,"
                        + " 10.3.68.48:9092, 10.3.68.50:9092,"
                        + " 10.3.68.52:9092")
                .option("subscribe", "rt-queue_1")
                .load();
        Dataset<Row> value = df.selectExpr("CAST(value AS STRING)");
        value = value.select(split(col("value"), "\t")
                                .getItem(0).cast("long")
                                .cast("timestamp").as("time"),
                        split(col("value"), "\t")
                                .getItem(6)
                                .cast("String")
                                .as("guid"),
                        split(col("value"), "\t")
                                .getItem(4)
                                .cast("String")
                                .as("bannerId"))
                .withColumn("Date", split(col("time"), " ")
                        .getItem(0))
                .withColumn("Hour", split(col("time"), " ")
                        .getItem(1))
                .drop(col("time"));

        value = value.withColumn("Day",
                        when(col("Hour").geq("06:00:00"), col("Date"))
                .when(col("Hour").leq("06:00:00"), date_sub(col("Date"), 1)))
                .drop(col("Date"));

        try {
            value.coalesce(1).writeStream().format("parquet")
                    .trigger(Trigger.ProcessingTime("1 hour"))
                    .outputMode("append")
                    .option("path",
                            "   hdfs://internship-hadoop105185:8120/mydata")
                    .option("checkpointLocation",
                            "hdfs://internship-hadoop105185:8120/checkpoint")
                    .partitionBy("Day")
                    .start().awaitTermination();

        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }



    }
}
