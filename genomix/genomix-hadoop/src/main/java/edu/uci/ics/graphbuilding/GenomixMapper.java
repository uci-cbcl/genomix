package edu.uci.ics.graphbuilding;

/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class implement mapper operator of mapreduce model
 */
public class GenomixMapper extends MapReduceBase implements Mapper<LongWritable, Text, VLongWritable, ValueWritable> {

    public static int KMER_SIZE;
    public ValueWritable outputAdjList = new ValueWritable();
    public VLongWritable outputKmer = new VLongWritable();

    @Override
    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
    }

    /*succeed node
      A 00000001 1
      G 00000010 2
      C 00000100 4
      T 00001000 8
      precursor node
      A 00010000 16
      G 00100000 32
      C 01000000 64
      T 10000000 128*/
    @Override
    public void map(LongWritable key, Text value, OutputCollector<VLongWritable, ValueWritable> output,
            Reporter reporter) throws IOException {
        /* A 00
           G 01
           C 10
           T 11*/
        String geneLine = value.toString(); // Read the Real Gene Line
        Pattern genePattern = Pattern.compile("[AGCT]+");
        Matcher geneMatcher = genePattern.matcher(geneLine);
        boolean isValid = geneMatcher.matches();
        int i = 0;
        if (isValid == true) {
            long kmerValue = 0;
            long PreMarker = -1;
            byte count = 0;
            //Get the next kmer by shiftint one letter every time
            for (i = 0; i < geneLine.length(); i++) {
                byte kmerAdjList = 0;
                if (i >= KMER_SIZE) {
                    outputKmer.set(kmerValue);
                    switch ((int) PreMarker) {
                        case -1:
                            kmerAdjList = (byte) (kmerAdjList + 0);
                            break;
                        case 0:
                            kmerAdjList = (byte) (kmerAdjList + 16);
                            break;
                        case 16:
                            kmerAdjList = (byte) (kmerAdjList + 32);
                            break;
                        case 32:
                            kmerAdjList = (byte) (kmerAdjList + 64);
                            break;
                        case 48:
                            kmerAdjList = (byte) (kmerAdjList + 128);
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
                }
                switch (geneLine.charAt(i)) {
                    case 'A':
                        kmerAdjList = (byte) (kmerAdjList + 1);
                        kmerValue = kmerValue + 0;
                        break;
                    case 'G':
                        kmerAdjList = (byte) (kmerAdjList + 2);
                        kmerValue = kmerValue + 1;
                        break;
                    case 'C':
                        kmerAdjList = (byte) (kmerAdjList + 4);
                        kmerValue = kmerValue + 2;
                        break;
                    case 'T':
                        kmerAdjList = (byte) (kmerAdjList + 8);
                        kmerValue = kmerValue + 3;
                        break;
                }
                if (i >= KMER_SIZE) {
                    outputAdjList.set(kmerAdjList, count);
                    output.collect(outputKmer, outputAdjList);
                }
                if (i < KMER_SIZE - 1)
                    kmerValue = (kmerValue << 2);
            }
            // arrive the last letter of this gene line
            if (i == geneLine.length()) {
                byte kmerAdjList = 0;
                switch ((int) PreMarker) {
                    case 0:
                        kmerAdjList = (byte) (kmerAdjList + 16);
                        break;
                    case 16:
                        kmerAdjList = (byte) (kmerAdjList + 32);
                        break;
                    case 32:
                        kmerAdjList = (byte) (kmerAdjList + 64);
                        break;
                    case 48:
                        kmerAdjList = (byte) (kmerAdjList + 128);
                        break;
                }
                outputAdjList.set(kmerAdjList, count);
                outputKmer.set(kmerValue);
                output.collect(outputKmer, outputAdjList);
            }
        }
    }
}