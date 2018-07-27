package dk.kb;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.rdd.RDD;
import org.junit.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class JavaWordCountTest extends SharedJavaSparkContext {
    
    //Inspiration from http://www.jesse-anderson.com/2016/04/unit-testing-spark-with-java/
    @Test
    public void verifyWordCount() {
        // Create and run the test
        
        //Generate input data
        List<String> input = Arrays.asList(
                "18/07/26 15:55:33 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 48 ms on localhost (executor driver) (1/1)",
                "18/07/26 15:55:33 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool",
                "18/07/26 15:55:33 INFO DAGScheduler: ResultStage 1 (collect at JavaWordCount.java:117) finished in 0.049 s",
                "18/07/26 15:55:33 INFO DAGScheduler: Job 0 finished: collect at JavaWordCount.java:117, took 1.578918 s");
        JavaRDD<String> inputRDD = jsc().parallelize(input);
        
        //Wordcount the input data
        JavaPairRDD<String, Integer> resultJavaPairRDD = JavaWordCount.wordCount(inputRDD);
        
        //for (Tuple2<String, Integer> stringIntegerTuple2 : resultJavaPairRDD.collect()) {
        //    System.out.println("new Tuple2<>(\""+stringIntegerTuple2._1()+"\","+stringIntegerTuple2._2()+"),");
        //}
        
        
        // Create the expected output
        List<Tuple2<String, Integer>> expectedInput = Arrays.asList(
                new Tuple2<>("removed", 1),
                new Tuple2<>("driver", 1),
                new Tuple2<>("00", 1),
                new Tuple2<>("have", 1),
                new Tuple2<>("0", 1),
                new Tuple2<>("whose", 1),
                new Tuple2<>("11", 1),
                new Tuple2<>("tasksetmanager", 1),
                new Tuple2<>("resultstage", 1),
                new Tuple2<>("all", 1),
                new Tuple2<>("dagscheduler", 2),
                new Tuple2<>("1", 2),
                new Tuple2<>("javawordcountjava117", 2),
                new Tuple2<>("from", 1),
                new Tuple2<>("collect", 2),
                new Tuple2<>("180726", 4),
                new Tuple2<>("finished", 3),
                new Tuple2<>("s", 2),
                new Tuple2<>("completed", 1),
                new Tuple2<>("executor", 1),
                new Tuple2<>("1578918", 1),
                new Tuple2<>("at", 2),
                new Tuple2<>("localhost", 1),
                new Tuple2<>("48", 1),
                new Tuple2<>("155533", 4),
                new Tuple2<>("pool", 1),
                new Tuple2<>("job", 1),
                new Tuple2<>("0049", 1),
                new Tuple2<>("taskset", 1),
                new Tuple2<>("in", 3),
                new Tuple2<>("task", 1),
                new Tuple2<>("tasks", 1),
                new Tuple2<>("taskschedulerimpl", 1),
                new Tuple2<>("info", 4),
                new Tuple2<>("ms", 1),
                new Tuple2<>("stage", 1),
                new Tuple2<>("on", 1),
                new Tuple2<>("took", 1),
                new Tuple2<>("tid", 1),
                new Tuple2<>("10", 2));
        JavaPairRDD<String, Integer> expectedJavaPairRDD = jsc().parallelizePairs(expectedInput);
        
        
        
        //We can only compare JavaRDDs, not JavaPairRDDs so we need to convert
        JavaRDD<Tuple2<String, Integer>> expectedRDD = javaPairRDD_to_JavaRDD(expectedJavaPairRDD);
        JavaRDD<Tuple2<String, Integer>> resultRDD = javaPairRDD_to_JavaRDD(resultJavaPairRDD);
    
        // Run the assertions on the resultRDD and expectedRDD
        JavaRDDComparisons.assertRDDEquals(expectedRDD, resultRDD);
    }
    
    private JavaRDD<Tuple2<String, Integer>> javaPairRDD_to_JavaRDD(JavaPairRDD<String, Integer> javaPairRDD) {
        //we create ClassTag object. This allows Scala to reflect the Object’s type correctly during the JavaPairRDD to JavaRDD conversion.
        ClassTag<Tuple2<String, Integer>> tag = scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);
    
        RDD<Tuple2<String, Integer>> RDDofTuples = JavaPairRDD.toRDD(javaPairRDD);
        
        return JavaRDD.fromRDD(RDDofTuples, tag);
    }
    
    
    @Test
    public void verifyPdfRead() throws IOException {
        
        //Load the sample file
        JavaPairRDD<String, PortableDataStream> pdfFiles = jsc().binaryFiles(Thread.currentThread()
                                                                                   .getContextClassLoader()
                                                                                   .getResource("samplePdf.pdf")
                                                                                   .getPath());
        //Extract the text
        JavaRDD<String> result = JavaWordCount.pdf2Text(pdfFiles);
        //Print it for debugging purposes
        printRDD(result);
        
        //Expected text
        JavaRDD<String> expectedRDD = jsc().parallelize(Arrays.asList("▪ eller timebox det\n"
                                                                      + "◦Møde-invitationen in\n"));

        // Run the assertions on the result and expected
        JavaRDDComparisons.assertRDDEquals(expectedRDD, result);
        
    }
    
    private void printRDD(JavaRDD<String> result) {
        for (String s : result.collect()) {
            System.out.println("'" + s + "'");
        }
    }
    
}