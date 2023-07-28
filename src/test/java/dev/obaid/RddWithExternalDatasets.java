package dev.obaid;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.stream.Stream;

public class RddWithExternalDatasets {
    private final SparkConf sparkConf = new SparkConf().setAppName("RddWithExternalDatasets").setMaster("local[*]");

    private static Stream<Arguments> getFilePaths() {
        return Stream.of(
                Arguments.of(Path.of("src/test/resources/1000words.txt").toString()),
                Arguments.of(Path.of("src/test/resources/wordslist.txt.gz").toString())
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "src/test/resources/1000words.txt",
            "src/test/resources/wordslist.txt.gz"
    })
    @DisplayName("Test loading local text file into spark RDD")
    void loadingLocalTxtFileInToSparkRdd(final String localFilePath) {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final JavaRDD<String> loadRdd = sparkContext.textFile(localFilePath);

            System.out.printf("The total number of lines in the file: %d%n", loadRdd.count());
            System.out.println("Take the first 10 lines");

            loadRdd.take(10).forEach(System.out::println);
            System.out.println("|------------ end of first 10 lines -------------|");
        }
    }

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test loading local text file into spark RDD with @MethodSource")
    void loadingLocalTxtFileInToSparkRddUsingMethodSource(final String localFilePath) {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final JavaRDD<String> loadRdd = sparkContext.textFile(localFilePath);

            System.out.printf("The total number of lines in the file: %d%n", loadRdd.count());
            System.out.println("Take the first 10 lines");

            loadRdd.take(10).forEach(System.out::println);
            System.out.println("|------------ end of first 10 lines -------------|\n");
        }
    }

    @Test
    void loadingWholeDirectoryFilesInToSparkRdd() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final String directoryPath = Path.of("src/test/resources").toString();
            // create rdds
            JavaPairRDD<String, String> wholeTextFilesRdd = sparkContext.wholeTextFiles(directoryPath);

            System.out.printf("The total number of files in the directory [ %s ] : %d%n", wholeTextFilesRdd, wholeTextFilesRdd.count());

            wholeTextFilesRdd.collect().forEach(tuple -> {
                System.out.printf("File name : %s%n", tuple._1);

                if (tuple._1.endsWith("properties")) {
                    System.out.printf("Content of [%s]:%n", tuple._1);
                    System.out.println(tuple._2);
                }
            });
        }
    }

}
