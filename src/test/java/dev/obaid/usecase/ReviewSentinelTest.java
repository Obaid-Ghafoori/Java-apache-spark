package dev.obaid.usecase;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * ReviewSentinel is a Java program that employs Apache Spark for analyzing online product reviews.
 * It counts the occurrences of unique positive and negative words within the reviews,
 * disregarding punctuations, numbers, and variations in word casing.
 * <p>
 * This program is designed for learning purposes and demonstrates the use of Spark for text analysis.
 * It showcases techniques for word extraction, cleaning, and counting to gain insights from textual data.
 */
public class ReviewSentinelTest {
    /**
     * Objective: Develop a Spark-based program to analyze online product reviews. Count the occurrences of unique positive
     * and negative words within the reviews, disregarding punctuations, numbers, and variations in word casing.
     */
    private static SparkConf sparkConf = new SparkConf().setAppName("ReviewSentinel").setMaster("local[*]");

    private static Stream<Arguments> getFilePath() {
        return Stream.of(Arguments.of(Path.of("src/test/resources/product_reviews.txt").toString()));
    }

    /**
     * Analyzes online product reviews to count the occurrences of positive and negative words.
     * This method demonstrates the use of Apache Spark for text analysis and showcases techniques
     * for word extraction, cleaning, and counting within reviews.
     *
     * @param reviewsFilePath The path to the file containing online product reviews.
     */
    @ParameterizedTest
    @MethodSource("getFilePath")
    void analyzeProductReviews(String reviewsFilePath) {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            // load data from the file
            JavaRDD<String> lineRdd = sc.textFile(reviewsFilePath);
            // Split each review into individual words based on space delimiters.
            JavaRDD<String> words = lineRdd.flatMap(wordIntheLine -> Arrays.asList(wordIntheLine.split("\\s")).iterator());


            // Create a set of positive and negative words that you'd like to analyze.
            Set<String> positiveWords = new HashSet<>(Arrays.asList(
                    "excellent",
                    "amazing",
                    "good",
                    "impressed",
                    "recommended"
            ));

            Set<String> negativeWords = new HashSet<>(Arrays.asList(
                    "poor",
                    "terrible",
                    "bad",
                    "disappointing",
                    "avoid",
                    "waste"
            ));

            Pattern noneWordPattern = Pattern.compile("[^a-zA-Z\\s]");

            JavaRDD<String> filterPositveReviews = words.map(word -> noneWordPattern.matcher(word).replaceAll(""))
                    .filter(word -> !word.isEmpty())
                    .map(String::toLowerCase);

            // count the occurrences of the Positive words
            JavaPairRDD<String, Integer> countOfPositveReviews = filterPositveReviews.filter(positiveWords::contains)
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey(Integer::sum);
            //print out the result
            System.out.println("---------| \u001B[32mPrinting of Positive Reviews:\u001B[0m |---------");
            countOfPositveReviews.collect().forEach(tuple -> System.out.printf("\t\t\t\t(%s,%d)%n", tuple._1, tuple._2));

            // count the occurrences of the Negative reviews
            JavaPairRDD<String, Integer> countOfNegativeReviews = filterPositveReviews.filter(negativeWords::contains)
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey(Integer::sum);
            //print out the result
            System.out.println("---------| \u001B[31mPrinting Negative Reviews\u001B[0m |---------");
            countOfNegativeReviews.collect().forEach(tuple -> System.out.printf("\t\t\t\t(%s,%d)%n", tuple._1, tuple._2));


        }

    }
}
