package dev.obaid;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateRddUsingParallelizeTest {
    private final SparkConf sparkConf = new SparkConf()
            .setAppName("Spark for java developer")
            .setMaster("local[*]");

    @Test
    @DisplayName("Create an empty RDD with number of partitions in Spark")
    void createAnEmptyRddWithNumberOfPartitionInSpark() {
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Object> emptyRdd = sparkContext.emptyRDD();
            System.out.println(emptyRdd);
            System.out.printf("Number of partitions: %d%n", emptyRdd.getNumPartitions());
        }
    }


    @Test
    @DisplayName("Create an empty RDD with default number of partitions in Spark")
    void createAnEmptyRddWithDefaultNumberOfPartitionInSpark() {
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Object> emptyRdd = sparkContext.parallelize(List.of());
            System.out.println(emptyRdd);
            System.out.printf("Number of partitions: %d%n", emptyRdd.getNumPartitions());
        }
    }

    @Test
    @DisplayName("Create spark RDD from Java collection using parallelize method")
    void createSparkRddUsingParallelize() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            List<Integer> inputData = Stream.iterate(1, number -> number + 1)
                    .limit(8)
                    .collect(Collectors.toList());
            final var defaultRdd = sparkContext.parallelize(inputData);
            System.out.println(defaultRdd);
            System.out.printf("Number of partitions: %d%n", defaultRdd.getNumPartitions());
            System.out.printf("Total number of element in RDD: %d%n", defaultRdd.count());
            System.out.println("Total number of element in RDD: ");
            defaultRdd.collect().forEach(System.out::println);

            //perform reduce operation
            Integer max = defaultRdd.reduce(Integer::max);
            Integer min = defaultRdd.reduce(Integer::min);
            Integer sum = defaultRdd.reduce(Integer::sum);

            System.out.printf("MAX-> %d,MIN-> %d, SUM ->%d%n", max, min, sum);

        }
    }


    @Test
    @DisplayName("Create spark RDD from Java collection using parallelize method with given partitions")
    void createSparkRddUsingParallelizeWithGivenPartitions() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            List<Integer> inputData = Stream.iterate(1, number -> number + 1)
                    .limit(8)
                    .collect(Collectors.toList());
            final var defaultRdd = sparkContext.parallelize(inputData, 10);
            System.out.println(defaultRdd);
            System.out.printf("Number of partitions: %d%n", defaultRdd.getNumPartitions());
            System.out.printf("Total number of element in RDD: %d%n", defaultRdd.count());
            System.out.println("Total number of element in RDD: ");
            defaultRdd.collect().forEach(System.out::println);

            //perform reduce operation
            Integer max = defaultRdd.reduce(Integer::max);
            Integer min = defaultRdd.reduce(Integer::min);
            Integer sum = defaultRdd.reduce(Integer::sum);

            System.out.printf("MAX-> %d,MIN-> %d, SUM ->%d%n", max, min, sum);

        }
    }
}
