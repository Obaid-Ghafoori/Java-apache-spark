package dev.obaid;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparkActionsTest {
    private static final List<Double> inputData = new ArrayList<>();
    private final SparkConf sparkConf = new SparkConf().setAppName("SparkActionsTest").setMaster("local[*]");


    @BeforeAll
    static void beforeAll() {
        final var dataSize = 1_000_000;
        for (int i = 0; i < dataSize; i++) {
            inputData.add(100 * ThreadLocalRandom.current().nextDouble() + 57);
        }
        assertEquals(dataSize, inputData.size());
    }

    @Test
    @DisplayName("Test reduce() in spark Rdd")
    void testReduceMethodInSparkRdd() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {

            //create Rdds
            JavaRDD<Double> doubleJavaRDD = sparkContext.parallelize(inputData, 14);

            final Instant statTime = Instant.now();
            for (int i = 0; i < 10; i++) {
                final Double sum = doubleJavaRDD.reduce(Double::sum);
                System.out.println("[Spark RDD with Reduce] -> SUM: " + sum);
            }
            final int noOfIterations = 10;
            final long timeElapsed = (Duration.between(statTime, Instant.now()).toMillis()) / noOfIterations;
            System.out.printf("[Spark RDD with Reduce] time taken: %d ms%n%n", timeElapsed);
        }

    }


    @Test
    @DisplayName("Test fold() in spark Rdd")
    void testFoldMethodInSparkRdd() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {

            //create Rdds
            JavaRDD<Double> doubleJavaRDD = sparkContext.parallelize(inputData, 14);

            final Instant statTime = Instant.now();
            for (int i = 0; i < 10; i++) {
                final Double sum = doubleJavaRDD.fold(0D, Double::sum);
                System.out.println("[Spark RDD with fold] -> SUM: " + sum);
            }
            final int noOfIterations = 10;
            final long timeElapsed = (Duration.between(statTime, Instant.now()).toMillis()) / noOfIterations;
            System.out.printf("[Spark RDD with fold] time taken: %d ms%n%n", timeElapsed);
        }

    }

    @Test
    @DisplayName("Test aggregate() in spark Rdd")
    void testAggregateMethodInSparkRdd() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {

            //create Rdds
            JavaRDD<Double> doubleJavaRDD = sparkContext.parallelize(inputData, 14);

            final Instant statTime = Instant.now();
            for (int i = 0; i < 10; i++) {
                final Double sum = doubleJavaRDD.aggregate(0D, Double::sum, Double::sum);
                final Double max = doubleJavaRDD.aggregate(0D, Double::sum, Double::max);
                final Double min = doubleJavaRDD.aggregate(0D, Double::sum, Double::min);
                System.out.println("[Spark RDD with aggregate] -> SUM: " + sum);
                System.out.println("[Spark RDD with aggregate] -> Max: for the partition " + max);
                System.out.println("[Spark RDD with aggregate] -> SUM: for the partition " + min);
            }
            final int noOfIterations = 10;
            final long timeElapsed = (Duration.between(statTime, Instant.now()).toMillis()) / noOfIterations;
            System.out.printf("[Spark RDD with aggregate] time taken: %d ms%n%n", timeElapsed);
        }

    }
}
