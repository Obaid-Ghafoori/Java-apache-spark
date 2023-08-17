package dev.obaid;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;


public class WideTransformationsTest {
    private final SparkConf sparkConf = new SparkConf().setAppName("WideTransformation").setMaster("local[*]");

    private static Stream<Arguments> getFilePaths() {
        return Stream.of(
                Arguments.of(Path.of("src/test/resources/1000words.txt").toString()),
                Arguments.of(Path.of("src/test/resources/wordslist.txt.gz").toString())
        );
    }

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test mapToPair() method in spark RDD")
    void testMapToPair(final String tesFilePath) {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            final JavaRDD<String> stringJavaRDD = sparkContext.textFile(tesFilePath);
            System.out.printf("Total lines of each file: %d%n", stringJavaRDD.count());

            final JavaPairRDD<Integer, String> integerStringJavaPairRDD = stringJavaRDD.mapToPair(wordInLine -> new Tuple2<>(wordInLine.length(), wordInLine));
            assertThat(stringJavaRDD.count()).isEqualTo(integerStringJavaPairRDD.count());

            integerStringJavaPairRDD.take(5).forEach(System.out::println);
            System.out.println("|------------------------------------------|");
        }
    }


    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test reduceByKey() method in spark RDD")
    void testMapToPairWithReduceByKey(final String tesFilePath) {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            final JavaRDD<String> stringJavaRDD = sparkContext.textFile(tesFilePath);
            System.out.printf("Total lines of each file: %d%n", stringJavaRDD.count());

            final JavaPairRDD<Integer, Long> integerLongJavaPairRDD = stringJavaRDD
                    .mapToPair(wordInLine -> new Tuple2<>(wordInLine.length(), 1L));
            assertThat(stringJavaRDD.count()).isEqualTo(integerLongJavaPairRDD.count());

            final JavaPairRDD<Integer, Long> countsOfPairedRDD = integerLongJavaPairRDD.reduceByKey(Long::sum);

            countsOfPairedRDD.take(5).forEach(tuple ->
                    System.out.printf("Total String of length %d are %d%n", tuple._1, tuple._2));
            System.out.println("|------------------------------------------|");
        }
    }

    /**
     * groupBy should be avoided as much as possible
     *
     * @param tesFilePath
     */
    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test groupBy() method in spark RDD")
    void testMapToPairWithGroupBy(final String tesFilePath) {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            final JavaRDD<String> stringJavaRDD = sparkContext.textFile(tesFilePath);
            System.out.printf("Total lines of each file: %d%n", stringJavaRDD.count());

            final JavaPairRDD<Integer, Long> integerLongJavaPairRDD = stringJavaRDD
                    .mapToPair(wordInLine -> new Tuple2<>(wordInLine.length(), 1L));
            assertThat(stringJavaRDD.count()).isEqualTo(integerLongJavaPairRDD.count());

            final JavaPairRDD<Integer, Iterable<Long>> countsOfPairedRDD = integerLongJavaPairRDD.groupByKey();

            countsOfPairedRDD.take(5).forEach(tuple ->
                    System.out.printf("Total String of length %d are %d%n", tuple._1, Iterables.size(tuple._2)));
            System.out.println("|---------------------------------|");
        }
    }

}
