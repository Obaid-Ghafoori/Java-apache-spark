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

import static org.apache.hadoop.shaded.org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * RDD transformations in Apache Spark are operations that create a new Resilient Distributed Dataset (RDD) from an existing RDD.
 * RDD transformations are executed lazily, meaning they are not actually computed until an action is triggered. Transformations
 * are the building blocks for defining the sequence of operations to be applied to distributed data.
 * <p>
 * RDD transformations allow you to specify how you want to transform or manipulate the data within the RDD.
 * They are performed in parallel across the partitions of the RDD and take advantage of Spark's distributed processing capabilities.
 */
public class RddTransformationsTest {
    private static final List<String> inputData = new ArrayList<>();
    private final SparkConf sparkConf = new SparkConf().setAppName("SparkTransformationTest").setMaster("local[*]");
    private final int noOfIterations = 10;

    @BeforeAll
    static void setup() {
        final long dataSize = 100_000L;
        for (int i = 0; i < dataSize; i++) {
            inputData.add(randomAscii(ThreadLocalRandom.current().nextInt(10)));
        }
        assertThat(inputData.size()).isEqualTo(dataSize);
    }

    @Test
    @DisplayName("Test map operation using spark rdd")
    void testMapOperationsUsingSparkRddCount() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            // create rdds
            JavaRDD<String> mapRdd = sparkContext.parallelize(inputData);

            final Instant startTime = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final long stringLength = mapRdd.map(String::length).count();
                assertThat(inputData.size()).isEqualTo(stringLength);
            }
            final long timeElapsed = (Duration.between(startTime, Instant.now()).toMillis()) / noOfIterations;
            System.out.printf("[Spark RDD with count()] time taken: %d ms%n%n", timeElapsed);
        }
    }

}
