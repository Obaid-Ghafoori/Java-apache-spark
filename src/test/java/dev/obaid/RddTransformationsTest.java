package dev.obaid;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
 * <p>
 * In this class Narrow transformations is considered. Narrow transformations are generally more efficient because t
 * hey avoid the costly process of shuffling data between different partitions. They can be executed in parallel
 * on each partition of the RDD without much overhead. On the other hand, wide transformations (such as groupBy or join)
 * require data to be shuffled and redistributed across partitions, which introduces more overhead due to data movement.
 */
public class RddTransformationsTest {
    private static final List<String> inputData = new ArrayList<>();
    private final SparkConf sparkConf = new SparkConf().setAppName("SparkTransformationTest").setMaster("local[*]");
    private final int noOfIterations = 10;

    @BeforeAll
    static void setup() {
        final var dataSize = 100_000L;
        for (int i = 0; i < dataSize; i++) {
            inputData.add(randomAscii(ThreadLocalRandom.current().nextInt(10)));
        }
        assertThat(inputData.size()).isEqualTo(dataSize);
    }

    /**
     * Return a new distributed dataset formed by passing each element of the source through a function func.
     */
    @Test
    @DisplayName("Test map operation using spark rdd-count")
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

    /**
     * Return all the elements of the dataset as an array at the driver program.
     * This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.
     */
    @Test
    @DisplayName("Test map operation using spark rdd-collect")
    void testMapOperationsUsingSparkRddCollect() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            final JavaRDD<String> mapRdd = sparkContext.parallelize(inputData);

            final Instant startTime = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final List<Integer> stringLength = mapRdd.map(String::length).collect();
                assertThat(inputData.size()).isEqualTo(stringLength.size());
            }
            final long timeElapsed = (Duration.between(startTime, Instant.now()).toMillis()) / noOfIterations;
            System.out.printf("[Spark RDD with collect()] time taken: %d ms%n%n", timeElapsed);
        }
    }

    /**
     * Aggregate the elements of the dataset using a function func (which takes two arguments and returns one).
     * The function should be commutative and associative so that it can be computed correctly in parallel.
     * <p>
     * It maps each element (which is a string) to its length using the String::length method reference.
     * The result is an RDD of integers representing the lengths of the strings.
     * .map(element -> 1): The second map transformation is applied to the RDD of string lengths obtained in the previous step.
     * Here, each length is mapped to the constant value 1. This effectively replaces each length with the value 1,
     * creating an RDD of integers where all values are 1.
     * <p>
     * the method calculates the count of elements in the RDD by first mapping each element to 1 and then using the reduce
     * action to add up these 1 values. The final result, stored in the stringLength variable, will be equal to the number of elements
     * in the mapRdd
     */
    @Test
    @DisplayName("Test map operation using spark rdd-reduce")
    void testMapOperationsUsingSparkRddReduce() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            final JavaRDD<String> mapRdd = sparkContext.parallelize(inputData);

            final Instant startTime = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final Integer stringLength = mapRdd.map(String::length)
                        .map(element -> 1)
                        .reduce(Integer::sum);
                assertThat(inputData.size()).isEqualTo(stringLength);
            }
            final long timeElapsed = (Duration.between(startTime, Instant.now()).toMillis()) / noOfIterations;
            System.out.printf("[Spark RDD with reduce()] time taken: %d ms%n%n", timeElapsed);
        }
    }


    /**
     * Similar to map, but each input item can be mapped to 0 or more output items
     * (so func should return a Seq rather than a single item).
     */

    @Test
    @DisplayName("Test flatMap operation using spark rdd-reduce")
    void testFlatMapOperationsUsingSparkRddReduce() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            final Path testFilePath = Path.of("src/test/resources/magna-carta.txt.gz");
            JavaRDD<String> lines = sparkContext.textFile(String.valueOf(testFilePath));

            System.out.printf("Total lines in file %d%n ", lines.count());
            final JavaRDD<List<String>> mapLines = lines.map(line -> List.of(line.split("\\s")));
            System.out.printf("Total lines in file %d%n ", mapLines.count());
            assertThat(lines.count()).isEqualTo(mapLines.count());

            final JavaRDD<String> words = lines.flatMap(line -> Arrays.stream(line.split("\\s"))
                    .map(String::trim)  // Trim each word
                    .filter(word -> !word.isEmpty())  // Filter out empty words
                    .iterator()); // convert the processed stream back into an iterator for the flatMap operation
            System.out.printf("Total number of words in file are: %d%n ", words.count());

            System.out.println("First few word..!");
            words.take(10).forEach(System.out::println);

        }
    }

    /**
     * use spark filter - Return a new dataset formed by selecting those elements of the source on which func returns true.
     */
    @Test
    @DisplayName("Test flatMap operation using spark rdd-reduce")
    void testFlatMapOperationsUsingSparkRddFilter() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            final Path testFilePath = Path.of("src/test/resources/magna-carta.txt.gz");
            JavaRDD<String> lines = sparkContext.textFile(String.valueOf(testFilePath));

            System.out.printf("Total lines in file %d%n ", lines.count());
            final JavaRDD<List<String>> mapLines = lines.map(line -> List.of(line.split("\\s")));
            System.out.printf("Total lines in file %d%n ", mapLines.count());
            assertThat(lines.count()).isEqualTo(mapLines.count());

            final JavaRDD<String> words = lines.flatMap(line -> List.of(line.split("\\s")).iterator());
            JavaRDD<String> notEmptyWords = words.filter(word -> (word != null) && (word.trim().length() > 0));
            System.out.printf("Total number of words in file after filtering are: %d%n ", notEmptyWords.count());

            assertThat(notEmptyWords.count()).isLessThan(words.count());

            System.out.println("First few word..!");
            notEmptyWords.take(10).forEach(System.out::println);

        }
    }
}


