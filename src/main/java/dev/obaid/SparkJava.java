package dev.obaid;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkJava {
    public static void main(String[] args) {
        try (
                final SparkSession sparkSession = SparkSession.builder().appName("Spark for java developers").master("local[*]").getOrCreate();
                final JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext())) {

            final List<Integer> data = Stream
                    .iterate(1, number -> number + 1)
                    .limit(6)
                    .collect(Collectors.toList());
            System.out.println("|--------------------------- Spark for java developer application get started -----------------------------------|");
            data.forEach(System.out::println);
            // create RDDs
            final JavaRDD<Integer> rdd = sparkContext.parallelize(data);
            //number of rdds and partation
            System.out.printf("the total number of RDDs: %d%n", rdd.count());
            System.out.printf("the default number of partitions: %d%n", rdd.getNumPartitions());

            Integer max = rdd.reduce(Integer::max);
            Integer min = rdd.reduce(Integer::min);
            Integer sum = rdd.reduce(Integer::sum);

            System.out.printf("MAX-> %d,MIN-> %d, SUM ->%d%n", max, min, sum);

            final Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }
    }
}