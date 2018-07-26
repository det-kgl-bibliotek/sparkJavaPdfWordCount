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
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        
        //Read all files in dir
        JavaPairRDD<String, PortableDataStream> pdfFiles = ctx.binaryFiles(args[0]);
    
    
        //Here we extract a region of text from each file
        //Flatmap, as we do want a simple RDD of text, not a nested structure
        JavaRDD<String> texts = pdfFiles.flatMap(new FlatMapFunction<Tuple2<String, PortableDataStream>, String>() {
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
                    return Arrays.asList(stripper.getTextForRegion("class1")).iterator();
                }
            }
        });

        //Split all text on space
        JavaRDD<String> words = texts.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        //Create pairs of word,1
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) {
                return new Tuple2<String, Integer>(word.trim(), 1);
            }
        });

        //Group on identical words and sum the integers
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        //Retrieve the data from cluster to local memory
        List<Tuple2<String, Integer>> output = counts.collect();
        //Now that output is local, we can work on it as we normally would
        
        //Wrap output in a normal arraylist, so we can sort it. The collected list is immutable, so sort fails
        output = new ArrayList<>(output);
        
        //First we sort it
        output.sort(Comparator.comparing(Tuple2::_2));
        
        // Then we print it
        for (Tuple2<?, ?> tuple : output) {
            System.out.println("'"+tuple._1() + "' : '" + tuple._2()+"'");
        }
        ctx.stop();
    }
}
