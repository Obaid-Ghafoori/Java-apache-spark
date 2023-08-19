package dev.obaid;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RddPrintingElementsTest {
    private static final List<Double> inputData = new ArrayList<>();
    private final SparkConf sparkConf = new SparkConf().setAppName("RddPrintingElementsTest").setMaster("local[*]");

    @BeforeAll
    static void beforeAll() {
        final var dataSize = 20;
        for (int i = 0; i < dataSize; i++) {
            inputData.add(100 * ThreadLocalRandom.current().nextDouble() + 46);
        }
        assertThat(dataSize).isEqualTo(inputData.size());
    }

    /**
     * this throws a spark exceptions because the RDDs are not in the master node,
     * but they are the cluster nodes
     */
    @Test
    @DisplayName("Test Spark RDD elements using only ForEach")
    void testSparkRddElementsUsingOnlyForeach() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            // create rdds
            JavaRDD<Double> parallelizeRdd = sparkContext.parallelize(inputData);
            Throwable exception = assertThrows(SparkException.class, () -> parallelizeRdd.foreach(System.out::println));

            assertThat(exception.getMessage()).isEqualTo("Task not serializable");

        }
    }

    /**
     * this print spark RDDs using collect in the master node will bring all the rdd from the cluster nodes
     * to the master node and then will be able printing elements
     * Note: normally using collect method is not the good candidate for big data because it will run out of resources
     * it is better to use take() instead
     */
    @Test
    @DisplayName("Test Spark RDD elements using only Foreach with collect()")
    void testSparkRddElementsUsingOnlyForEachWithCollect() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            // create rdds
            JavaRDD<Double> parallelizeRdd = sparkContext.parallelize(inputData);

            final Instant startTime = Instant.now();
            parallelizeRdd.collect().forEach(System.out::println);
            final long timeElapsed = (Duration.between(startTime, Instant.now()).toMillis());
            System.out.printf("[Spark RDD with collect()] time taken: %d ms%n%n", timeElapsed);

        }
    }

    @Test
    @DisplayName("Test Spark RDD elements using only Foreach with take()")
    void testSparkRddElementsUsingOnlyForEachWithTake() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            // create rdds
            JavaRDD<Double> parallelizeRdd = sparkContext.parallelize(inputData);

            final Instant startTime = Instant.now();
            parallelizeRdd.take(10).forEach(System.out::println);
            final long timeElapsed = (Duration.between(startTime, Instant.now()).toMillis());
            System.out.printf("[Spark RDD with take()] time taken: %d ms%n%n", timeElapsed);

        }
    }
}
