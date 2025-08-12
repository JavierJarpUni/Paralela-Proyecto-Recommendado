// SparkDataFrameProcessor.java
package main.java.com.bigdata.processor;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;
import com.google.cloud.storage.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class SparkDataFrameProcessor {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: SparkDataFrameProcessor <inputPath> <outputPath> <jobId>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("BigData DataFrame Processor")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate();

        try {
            String inputPath = args[0];
            String outputPath = args[1];
            String jobId = args[2];

            long startTime = System.currentTimeMillis();

            // Define schema
            StructType schema = DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("value", DataTypes.DoubleType, true)
            });

            // Load data as DataFrame
            Dataset<Row> rawDF = spark.read()
                    .option("header", "false")
                    .schema(schema)
                    .csv(inputPath)
                    .cache();

            // Basic statistics
            Dataset<Row> statsDF = rawDF.select(
                    count("value").as("record_count"),
                    avg("value").as("avg_value"),
                    max("value").as("max_value"),
                    min("value").as("min_value"),
                    stddev("value").as("std_dev"));

            Row stats = statsDF.collect()[0];
            long recordCount = stats.getAs("record_count");
            double avgValue = stats.getAs("avg_value");
            double maxValue = stats.getAs("max_value");
            double minValue = stats.getAs("min_value");
            double stdDev = stats.getAs("std_dev");

            // Normalization and categorization using DataFrame API
            Dataset<Row> normalizedDF = rawDF
                    .withColumn("normalized_value",
                            col("value").minus(lit(avgValue)).divide(lit(stdDev)))
                    .withColumn("category",
                            when(col("normalized_value").gt(1.0), "high")
                                    .when(col("normalized_value").lt(-1.0), "low")
                                    .otherwise("medium"));

            // Aggregations by category
            Dataset<Row> categoryStatsDF = normalizedDF
                    .groupBy("category")
                    .agg(
                            count("*").as("count"),
                            avg("normalized_value").as("avg_normalized"),
                            avg("value").as("avg_original"));

            Row[] categoryStatsArray = (Row[]) categoryStatsDF.collect();

            long endTime = System.currentTimeMillis();
            long processingTime = endTime - startTime;

            // Prepare result
            Map<String, Object> result = new HashMap<>();
            result.put("job_id", jobId);
            result.put("processing_time_ms", processingTime);
            result.put("record_count", recordCount);
            result.put("avg_value", avgValue);
            result.put("max_value", maxValue);
            result.put("min_value", minValue);
            result.put("std_dev", stdDev);
            result.put("processing_type", "DataFrame");

            // Add category statistics
            List<Map<String, Object>> categoryStatsList = new ArrayList<>();
            for (Row row : categoryStatsArray) {
                Map<String, Object> categoryStats = new HashMap<>();
                categoryStats.put("category", row.getAs("category"));
                categoryStats.put("count", row.getAs("count"));
                categoryStats.put("avg_normalized", row.getAs("avg_normalized"));
                categoryStats.put("avg_original", row.getAs("avg_original"));
                categoryStatsList.add(categoryStats);
            }
            result.put("category_stats", categoryStatsList);

            // Save result
            saveResultToGCS(result, outputPath, jobId);

            System.out.println("DataFrame Processing completed for job: " + jobId);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    private static void saveResultToGCS(Map<String, Object> result,
            String outputPath,
            String jobId) {
        try {
            Storage storage = StorageOptions.getDefaultInstance().getService();

            ObjectMapper mapper = new ObjectMapper();
            String resultJson = mapper.writeValueAsString(result);

            BlobId blobId = BlobId.of("bigdata-processing-results", "spark_dataframe/" + jobId + ".json");
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                    .setContentType("application/json")
                    .build();
            storage.create(blobInfo, resultJson.getBytes());

        } catch (Exception e) {
            System.err.println("Failed to save results to GCS: " + e.getMessage());
            e.printStackTrace();
        }
    }
}