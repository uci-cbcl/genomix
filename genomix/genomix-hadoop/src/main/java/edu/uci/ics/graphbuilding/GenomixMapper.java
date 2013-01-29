package edu.uci.ics.graphbuilding;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class GenomixMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable> {

    public static final int KMER_SIZE = 3; //User Specify
    //    private Text Map_Pair_Key = new Text();

    /*precursor node
      A 00000001 1
      G 00000010 2
      C 00000100 4
      T 00001000 8
      succeed node
      A 00010000 16
      G 00100000 32
      C 01000000 64
      T 10000000 128*/
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, IntWritable> output, Reporter reporter)
            throws IOException {
        /* A 00
           G 01
           C 10
           T 11*/
        try {
            String geneLine = value.toString(); // Read the Real Gene Line
            Pattern genePattern = Pattern.compile("[AGCT]+");
            Matcher geneMatcher = genePattern.matcher(geneLine);
            boolean isValid = geneMatcher.matches();
            if (isValid == true) {
                long kmerValue = 0;
                long PreMarker = -1;
                //Initialization: get the first kmer of this geneLine 
                for (int i = 0; i < KMER_SIZE; i++) {
                    kmerValue = (kmerValue << 2);
                    switch (geneLine.charAt(i)) {
                        case 'A':
                            kmerValue = kmerValue + 0;
                            break;
                        case 'G':
                            kmerValue = kmerValue + 1;
                            break;
                        case 'C':
                            kmerValue = kmerValue + 2;
                            break;
                        case 'T':
                            kmerValue = kmerValue + 3;
                            break;
                    }
                }
                int i;
                //Get the next kmer by shiftint one letter every time
                for (i = KMER_SIZE; i < geneLine.length(); i++) {
                    LongWritable outputKmer = new LongWritable(kmerValue);
                    int kmerAdjList = 0;
                    //Get the precursor node using the premarker
                    switch ((int) PreMarker) {
                        case -1:
                            kmerAdjList = kmerAdjList + 0;
                            break;
                        case 0:
                            kmerAdjList = kmerAdjList + 16;
                            break;
                        case 16:
                            kmerAdjList = kmerAdjList + 32;
                            break;
                        case 32:
                            kmerAdjList = kmerAdjList + 64;
                            break;
                        case 48:
                            kmerAdjList = kmerAdjList + 128;
                            break;
                    }
                    //Update the premarker
                    PreMarker = 3;
                    PreMarker = PreMarker << (KMER_SIZE - 1) * 2;
                    PreMarker = PreMarker & kmerValue;
                    //Reset the top two bits
                    long reset = 3;
                    kmerValue = kmerValue << 2;
                    reset = ~(reset << KMER_SIZE * 2);
                    kmerValue = kmerValue & reset;
                    switch (geneLine.charAt(i)) {
                        case 'A':
                            kmerAdjList = kmerAdjList + 1;
                            kmerValue = kmerValue + 0;
                            break;
                        case 'G':
                            kmerAdjList = kmerAdjList + 2;
                            kmerValue = kmerValue + 1;
                            break;
                        case 'C':
                            kmerAdjList = kmerAdjList + 4;
                            kmerValue = kmerValue + 2;
                            break;
                        case 'T':
                            kmerAdjList = kmerAdjList + 8;
                            kmerValue = kmerValue + 3;
                            break;
                    }
                    IntWritable outputAdjList = new IntWritable(kmerAdjList);
                    output.collect(outputKmer, outputAdjList);
                }
                // arrive the last letter of this gene line
                if (i == geneLine.length()) {
                    int kmerAdjList = 0;
                    switch ((int) PreMarker) {
                        case 0:
                            kmerAdjList = kmerAdjList + 16;
                            break;
                        case 16:
                            kmerAdjList = kmerAdjList + 32;
                            break;
                        case 32:
                            kmerAdjList = kmerAdjList + 64;
                            break;
                        case 48:
                            kmerAdjList = kmerAdjList + 128;
                            break;
                    }
                    IntWritable outputAdjList = new IntWritable(kmerAdjList);
                    LongWritable outputKmer = new LongWritable(kmerValue);
                    output.collect(outputKmer, outputAdjList);
                }
            }
        } catch (Exception e) {
            System.out.println("Exception:" + e);
        }
    }
}
