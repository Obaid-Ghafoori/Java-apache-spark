package dev.obaid.usecase;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.regex.Pattern;

public class UniqueEnglishWordsTest {
    private final SparkConf sparkConf = new SparkConf().setAppName("ExerciseUniqueEnglishWords").setMaster("local[*]");

    /**
     * This method analyzes a text file to count unique English words using the Apache Spark framework.
     * It reads the specified file, processes its contents, and outputs word counts.
     */
    @Test
    void countEnglishUniqueWords() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            //path of the file
            final String pathToFile = Path.of("src/test/resources/magna-carta.txt.gz").toString();

            // load the text file
            JavaRDD<String> linesRdd = sparkContext.textFile(pathToFile);
            // get total words counts in the file
            System.out.println("__________________________________________");
            System.out.println("| Total word counts in the file is: " + linesRdd.count() + " |");
            System.out.println("__________________________________________");

            Pattern nonEnglishWord = Pattern.compile("[^a-zA-Z]");
            // convert lines into the list of words, filter out the non-english word, and make it case-insensitive
            JavaRDD<String> words = linesRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                    .map(word -> nonEnglishWord.matcher(word).replaceAll(""))
                    .filter(word -> !word.isEmpty() || !word.isBlank())
                    .map(String::toLowerCase);

            // convert it to key value and get the sum of each word
            JavaPairRDD<String, Integer> uniqueWordCount = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey(Integer::sum);

            // print out the top 10 words
            uniqueWordCount.take(10).forEach(tuple -> System.out.println("\t\t\t(" + tuple._1 + ", " + tuple._2 + ")"));
            System.out.println("__________________________________________");
            System.out.println("|                End                      |");
            System.out.println("__________________________________________");

        }

    }


    /**
     * This test method demonstrates the use of Apache Spark to analyze a text file containing English words.
     * It counts the occurrences of unique English words while considering case-insensitivity and filtering out non-English words.
     * Additionally, it prints the total word count in the file and the top 10 most frequent words along with their counts.
     */

    @Test
    void countToTenEnglishUniqueWordsWithMaxCount() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            //path of the file
            final String pathToFile = Path.of("src/test/resources/magna-carta.txt.gz").toString();

            // load the text file
            JavaRDD<String> linesRdd = sparkContext.textFile(pathToFile);
            // get total words counts in the file
            System.out.println("__________________________________________");
            System.out.println("| Total word counts in the file is: " + linesRdd.count() + " |");
            System.out.println("------------------------------------------");

            Pattern nonEnglishWord = Pattern.compile("[^a-zA-Z]");
            // convert lines into the list of words, filter out the non-english word, and make it case-insensitive
            JavaRDD<String> words = linesRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                    .map(word -> nonEnglishWord.matcher(word).replaceAll(""))
                    .filter(word -> !word.isEmpty() || !word.isBlank())
                    .map(String::toLowerCase);

            // convert it to key value and get the sum of each word
            JavaPairRDD<String, Integer> uniqueWordCount = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey(Integer::sum);
            uniqueWordCount.take(10).forEach(tuple -> System.out.println("\t\t\t(" + tuple._1 + ", " + tuple._2 + ")"));

            System.out.println("----------Printing the top 10 max words-------");
            // print out the top 10 words with maximum counts
            uniqueWordCount.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                    .sortByKey(false)
                    .take(10)
                    .forEach(tuple -> System.out.printf("\t\t\t(%s,%d)%n", tuple._2, tuple._1));
            System.out.println("------------------------------------------");


        }

    }
}

