// SparkRDDProcessor.java
package main.java.com.bigdata.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import com.google.cloud.storage.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.stream.Collectors;

public class SparkRDDProcessor {

    public static class ProcessingResult {
        @JsonProperty("job_id")
        private String jobId;

        @JsonProperty("processing_time_ms")
        private long processingTime;

        @JsonProperty("record_count")
        private long recordCount;

        @JsonProperty("avg_value")
        private double avgValue;

        @JsonProperty("max_value")
        private double maxValue;

        @JsonProperty("min_value")
        private double minValue;

        // Constructors, getters, and setters
        public ProcessingResult() {
        }

        public ProcessingResult(String jobId, long processingTime, long recordCount,
                double avgValue, double maxValue, double minValue) {
            this.jobId = jobId;
            this.processingTime = processingTime;
            this.recordCount = recordCount;
            this.avgValue = avgValue;
            this.maxValue = maxValue;
            this.minValue = minValue;
        }

        // Getters and setters
        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public long getProcessingTime() {
            return processingTime;
        }

        public void setProcessingTime(long processingTime) {
            this.processingTime = processingTime;
        }

        public long getRecordCount() {
            return recordCount;
        }

        public void setRecordCount(long recordCount) {
            this.recordCount = recordCount;
        }

        public double getAvgValue() {
            return avgValue;
        }

        public void setAvgValue(double avgValue) {
            this.avgValue = avgValue;
        }

        public double getMaxValue() {
            return maxValue;
        }

        public void setMaxValue(double maxValue) {
            this.maxValue = maxValue;
        }

        public double getMinValue() {
            return minValue;
        }

        public void setMinValue(double minValue) {
            this.minValue = minValue;
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: SparkRDDProcessor <inputPath> <outputPath> <jobId>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf()
                .setAppName("BigData RDD Processor")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            String inputPath = args[0];
            String outputPath = args[1];
            String jobId = args[2];

            long startTime = System.currentTimeMillis();

            // Load preprocessed data from Cloud Storage
            JavaRDD<String> textRDD = sc.textFile(inputPath);
            JavaRDD<Double> dataRDD = textRDD
                    .map(line -> {
                        try {
                            return Double.parseDouble(line.trim());
                        } catch (NumberFormatException e) {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .cache();

            // Basic statistics with RDD
            long count = dataRDD.count();
            double sum = dataRDD.reduce(Double::sum);
            double avg = sum / count;
            double max = dataRDD.max(Double::compare);
            double min = dataRDD.min(Double::compare);

            // Calculate standard deviation
            double variance = dataRDD
                    .map(value -> Math.pow(value - avg, 2))
                    .reduce(Double::sum) / count;
            double stdDev = Math.sqrt(variance);

            // Normalization and categorization
            JavaRDD<Double> normalizedRDD = dataRDD.map(value -> (value - avg) / stdDev);

            JavaPairRDD<String, Double> categorizedRDD = normalizedRDD.mapToPair(value -> {
                String category;
                if (value > 1.0) {
                    category = "high";
                } else if (value < -1.0) {
                    category = "low";
                } else {
                    category = "medium";
                }
                return new Tuple2<>(category, value);
            });

            // Aggregations by category
            Map<String, Tuple2<Integer, Double>> categoryStats = categorizedRDD
                    .groupByKey()
                    .mapValues(values -> {
                        List<Double> valueList = new ArrayList<>();
                        values.forEach(valueList::add);
                        double categorySum = valueList.stream().mapToDouble(Double::doubleValue).sum();
                        double categoryAvg = categorySum / valueList.size();
                        return new Tuple2<>(valueList.size(), categoryAvg);
                    })
                    .collectAsMap();

            long endTime = System.currentTimeMillis();
            long processingTime = endTime - startTime;

            ProcessingResult result = new ProcessingResult(
                    jobId, processingTime, count, avg, max, min);

            // Save results to Google Cloud Storage
            saveResultToGCS(result, categoryStats, outputPath, jobId);

            System.out.println("RDD Processing completed: " + result.getJobId());

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            sc.stop();
        }
    }

    private static void saveResultToGCS(ProcessingResult result,
            Map<String, Tuple2<Integer, Double>> categoryStats,
            String outputPath,
            String jobId) {
        try {
            Storage storage = StorageOptions.getDefaultInstance().getService();

            // Create result JSON
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("job_id", result.getJobId());
            resultMap.put("processing_time_ms", result.getProcessingTime());
            resultMap.put("record_count", result.getRecordCount());
            resultMap.put("avg_value", result.getAvgValue());
            resultMap.put("max_value", result.getMaxValue());
            resultMap.put("min_value", result.getMinValue());
            resultMap.put("processing_type", "RDD");

            // Add category statistics
            Map<String, Map<String, Object>> categoryStatsMap = new HashMap<>();
            for (Map.Entry<String, Tuple2<Integer, Double>> entry : categoryStats.entrySet()) {
                Map<String, Object> stats = new HashMap<>();
                stats.put("count", entry.getValue()._1());
                stats.put("average", entry.getValue()._2());
                categoryStatsMap.put(entry.getKey(), stats);
            }
            resultMap.put("category_stats", categoryStatsMap);

            // Convert to JSON
            ObjectMapper mapper = new ObjectMapper();
            String resultJson = mapper.writeValueAsString(resultMap);

            // Save to Cloud Storage
            BlobId blobId = BlobId.of("bigdata-processing-results", "spark_rdd/" + jobId + ".json");
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