package code;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.swoop.alchemy.spark.expressions.hll.functions.hll_cardinality;
import static com.swoop.alchemy.spark.expressions.hll.functions.hll_merge;
import static com.swoop.alchemy.spark.expressions.hll.functions.hll_init_agg;
import static org.apache.spark.sql.functions.col;

public class GetResult {

    private static  String hdfsPath = "hdfs://internship-hadoop105185:8120/mydata/";
    private static String date = "2022-05-29";
    private static String path = hdfsPath + "Day=" + date + "/*";
    /*
    *Ham main
    */
    public static void main(final String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("yarn")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark.read().parquet(path);
        Dataset<Row> res = df.groupBy(col("bannerId"))
                .agg(hll_init_agg("guid")
                        .as("guid_hll"))
                .groupBy("bannerId").agg(hll_merge("guid_hll").as("guid_hll"));
        res.select(col("bannerId"),
                        hll_cardinality("guid_hll"))
                .show(false);
    }
}
