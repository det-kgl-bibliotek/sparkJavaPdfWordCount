package dk.kb;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class JavaWordCount {
    
    private static final Pattern SPACE = Pattern.compile("\\s+");
    
    
    public static void main(String[] args) throws Exception {
        
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <path>");
            System.exit(1);
        }
        
        
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        
        //Configure how many resources we need from the cluster
        configureResourceAllocation(sparkConf);
    
    
        //Of of the many ways to reduce spark logging
        //LogManager.getLogger("org").setLevel(Level.WARN);
        
        try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {
    
    
            //Read all files in dir
            JavaPairRDD<String, PortableDataStream> pdfFiles = ctx.binaryFiles(args[0]);
            
            //Extract text from pdf files
            JavaRDD<String> texts = pdf2Text(pdfFiles);
    
            //Perform the word count
            JavaPairRDD<String, Integer> counts = wordCount(texts);
        
            //Retrieve the data from cluster to local memory
            List<Tuple2<String, Integer>> output = counts.collect();
            //Now that output is local, we can work on it as we normally would
    
            //Wrap output in a normal arraylist, so we can sort it. The collected list is immutable, so sort fails
            output = new ArrayList<>(output);
    
            //First we sort it
            output.sort(Comparator.comparing(Tuple2::_2));
    
            // Then we print it
            for (Tuple2<?, ?> tuple : output) {
                System.out.println("'" + tuple._1() + "' : '" + tuple._2() + "'");
            }
        }
    }
    
    /**
     * Set Spark configuration
     * This can also be done from on the command line and through a properties file.
     * @param sparkConf The spark configuration object to configure
     * @return the configured sparkConf object, in case you need it...
     */
    protected static SparkConf configureResourceAllocation(SparkConf sparkConf) {
        //These apparently only function when we use master=yarn property
        //These properties can be set in a config file or as command line params,
        // or in the code, as seen here
        
        //Hver executor skal have 25GB RAM (default setting for KAC)
        // Set ned til 2G hvis du skal køre test
        //sparkConf.set("spark.executor.memory", "2G");
        
        //Og hver executor skal bruge 4 kerner (default).
        //Sæt ned til 1 hvis du skal køre test.
        //sparkConf.set("spark.executor.cores", "1");
        
        //Vi kan max have 26 executors
        sparkConf.set("spark.dynamicAllocation.maxExecutors", "26");
        //Og min 1 executor
        sparkConf.set("spark.dynamicAllocation.minExecutors", "0");
        //Og vi starter med 1
        sparkConf.set("spark.dynamicAllocation.initialExecutors", "3");
        
        return sparkConf;
    }
    
    protected static JavaRDD<String> pdf2Text(JavaPairRDD<String, PortableDataStream> pdfFiles) throws IOException {
        //Here we extract a region of text from each file
        //Flatmap, as we do want a simple RDD of text, not a nested structure
        return pdfFiles.flatMap(new FlatMapFunction<Tuple2<String, PortableDataStream>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, PortableDataStream> tuple)
                    throws Exception {
                try (PDDocument document = PDDocument.load(tuple._2.open())) {
                    //        Example pdf operation
                    PDFTextStripperByArea stripper = new PDFTextStripperByArea();
                    stripper.setSortByPosition(true);
                    Rectangle rect = new Rectangle(10, 280, 275, 60);
                    stripper.addRegion("class1", rect);
                    PDPage firstPage = document.getPage(0);
                    stripper.extractRegions(firstPage);
                    String text = stripper.getTextForRegion("class1");
                    return Arrays.asList(text).iterator();
                }
            }
        });
    }
    
    protected static JavaPairRDD<String, Integer> wordCount(JavaRDD<String> texts) {
        //Split all text on space
        JavaRDD<String> words = texts.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });
        
        //clean words
        JavaRDD<String> cleaned_words = words.map(
                s -> s.trim()
                      .toLowerCase()
                      .replaceAll("\\W", "")
        ).filter(s -> s != null && !s.isEmpty());
        
        
        //Create pairs of word,1
        JavaPairRDD<String, Integer> ones = cleaned_words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) {
                return new Tuple2<String, Integer>(word.trim(), 1);
            }
        });
        
        //Group on identical words and sum the integers
        return ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
    }
}
