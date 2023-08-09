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

public class RddWithExternalDatasetsTest {
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
    @DisplayName("loading whole directory files into Spark RDD")
    void loadingWholeDirectoryFilesInToSparkRdd() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final String directoryPath = Path.of("src/test/resources").toString();
            // create rdds
            JavaPairRDD<String, String> wholeTextFilesRdd = sparkContext.wholeTextFiles(directoryPath);

            System.out.printf("The total number of files in the directory [ %s ] : %d%n", directoryPath, wholeTextFilesRdd.count());

            // Print the file names first
            wholeTextFilesRdd.keys().foreach(fileName -> System.out.printf("File name: %s%n", fileName));

            // Filter the RDD to keep only the files ending with "properties" and print their content
            JavaPairRDD<String, String> propertiesFilesRdd = wholeTextFilesRdd.filter(tuple -> tuple._1.endsWith("properties"));

            propertiesFilesRdd.foreach(tuple -> {
                System.out.printf("Content of [%s]:%n", tuple._1);
                System.out.println(tuple._2);
            });
        }

    }


    @Test
    @DisplayName("loading CSV file into Spark RDD")
    void loadingCsvFilesInToSparkRdd() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final String csvFilePath = Path.of("src/test/resources/dma.csv").toString();
            // create rdds
            JavaRDD<String> csvFileRdd = sparkContext.textFile(csvFilePath);

            System.out.printf("The total number of files in the directory [ %s ] : %d%n", csvFilePath, csvFileRdd.count());

            System.out.println("CSV file headers ->");
            System.out.println(csvFileRdd.first());
            System.out.println("|--------------------|");

            System.out.println("CSV file headers ->");
            csvFileRdd.take(10).forEach(System.out::println);
            System.out.println("|--------------------|");

            //extract each flied from csv
            JavaRDD<String[]> csvFields = csvFileRdd.map(line -> line.split(","));

            // take the first 5 lines and separted with the '|'
            csvFields.take(5).forEach(field -> System.out.println(String.join(" | ", field)));
        }

    }

}
