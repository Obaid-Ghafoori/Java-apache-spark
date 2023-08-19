package dev.obaid;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class RddTuplesTest {
    private static final List<Double> inputData = new ArrayList<>();
    private final SparkConf sparkConf = new SparkConf().setAppName("SparkTuplesTest").setMaster("local[*]");


    private static Stream<Arguments> getfilePaths() {
        return Stream.of(Arguments.of(Path.of("src/test/resources/1000words.txt").toString()),
                Arguments.of(Path.of("src/test/resources/wordsList.txt.gz").toString()));
    }

    /**
     *
     */
    @Test
    @DisplayName("Test Spark RDD elements using only ForEach")
    void testSparkRddElementsUsingOnlyForeach() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            // create rdds

        }
    }

    /**
     *
     */
    @Test
    @DisplayName("Test Spark RDD elements using only Foreach with collect()")
    void testSparkRddElementsUsingOnlyForEachWithCollect() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            // create rdds
            JavaRDD<Double> parallelizeRdd = sparkContext.parallelize(inputData);

        }
    }

    @ParameterizedTest
    @MethodSource("getfilePaths")
    @DisplayName("Test tuples in Spark RDD")
    void testSparkRddElementsUsingOnlyForEachWithTake(final String testFilePath) {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            // create rdds
            JavaRDD<String> textFileRdd = sparkContext.textFile(testFilePath);
            System.out.printf("Total lines in the file: %d%n ", textFileRdd.count());

            JavaRDD<Tuple2> tuple2JavaRDD = textFileRdd.map(line -> new Tuple2<>(line, line.length()));
            assertThat(textFileRdd.count()).isEqualTo(tuple2JavaRDD.count());
            tuple2JavaRDD.take(10).forEach(System.out::println);
            System.out.println("----------------------------------");
        }
    }
}
