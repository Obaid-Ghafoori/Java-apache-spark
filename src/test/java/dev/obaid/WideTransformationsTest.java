package dev.obaid;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class WideTransformationsTest {
    private final SparkConf sparkConf = new SparkConf().setAppName("WideTransformation").setMaster("local[*]");

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test mapToPair() method in spark RDD")
    void testMapToPair(final String tesFilePath) {
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {

        }
    }


}
