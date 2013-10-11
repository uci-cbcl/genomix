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

package edu.uci.ics.genomix.hadoop.gage;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hadoop.gage.IntPair.FirstPartitioner;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

public class GetFastaStatsJob {
    
    /**
     * use secondary sort, before all elements go to reducer
     */
    @SuppressWarnings("deprecation")
    public static class GetFastaStatsMapper extends MapReduceBase implements Mapper<VKmer, Node, IntPair, IntWritable> {

        private IntPair outputKey = new IntPair();
        private IntWritable outputValue = new IntWritable();
        private static final String[] suffix = { "final", "split", "fasta", "scafSeq", "fa" };

        //        public static int MIN_CONTIG_LENGTH;
        //        public static int EXPECTED_GENOME_SIZE;

        private boolean isExtensionInSuffix(String extension) {
            for (int i = 0; i < suffix.length; i++) {
                if (extension.equals(suffix[i]))
                    return true;
            }
            return false;
        }

        @Override
        public void configure(JobConf job) {
            String filename = job.get("map.input.file");
            String[] tokens = filename.split("\\.(?=[^\\.]+$)");
            if (tokens.length != 2)
                throw new IllegalStateException("Can not find the  extension tokens are: " + tokens.toString());
            String extension = tokens[1];
            if (!isExtensionInSuffix(extension)) {
                throw new IllegalStateException("Can not parse the kind of result file");
            }
            //                        TODO MIN_CONTIG_LENGTH = Integer.parseInt(job.get(GenomixJobConf.MIN_CONTIG_LENGTH));
            //                        TODO EXPECTED_GENOME_SIZE = Integer.parseInt(job.get(GenomixJobConf.EXPECTED_GENOMIX_SIZE));
            //            MIN_CONTIG_LENGTH = 25; //TODO for test
            //            EXPECTED_GENOME_SIZE = 150; //TODO for test

        }

        @Override
        public void map(VKmer key, Node value, OutputCollector<IntPair, IntWritable> output, Reporter reporter)
                throws IOException {
            //            String fastaString = value.getInternalKmer().toString().replaceAll("-", "");
            //            fastaString = fastaString.toString().replaceAll("\\.", "");
            outputKey.set(0, value.getInternalKmer().getKmerLetterLength());
            outputValue.set(value.getInternalKmer().getKmerLetterLength());

            output.collect(outputKey, outputValue);
        }
    }

    @SuppressWarnings("deprecation")
    public static class GetFastaStatsReducer extends MapReduceBase implements
            Reducer<IntPair, IntWritable, Text, NullWritable> {
        

        private ArrayList<Integer> contigLengthList;

        public static boolean USE_BAYLOR_FORMAT;
        public static boolean OLD_STYLE;
        public static int MIN_CONTIG_LENGTH;
        public static int EXPECTED_GENOME_SIZE;

        int maxContig;
        int minContig;
        long totalContigLength;
        int contigCount;
        int n10;
        int n25;
        int n50; // is the size of the smallest contig such that 50% of the genome is contained in contigs of size N50 or larger
        int n75;
        int n95;
        int n10count;
        int n25count;
        int n50count;
        int n75count;
        int n95count;
        int totalOverLength;
        long totalBPOverLength;
        double fSize; // Esize what's expect contig size, if we randomly choose one position in reference genome

        private static final int CONTIG_AT_INITIAL_STEP = 1000000; //TODO ??? I am relly confused this which related to baylor format
        private static final NumberFormat nf = new DecimalFormat("############.##");

        private NullWritable nullWritable;
        private Text outKey;
        private StringBuffer tempKey;

        private class ContigAt {
            ContigAt(long currentBases) {
                this.count = this.len = 0;
                this.totalBP = 0;
                this.goal = currentBases;
            }

            public int count;
            public long totalBP;
            public int len;
            public long goal;
        }

        @Override
        public void configure(JobConf job) {
            USE_BAYLOR_FORMAT = Boolean.parseBoolean(job.get(GenomixJobConf.USE_BAYLOR_FORMAT));
            OLD_STYLE = Boolean.parseBoolean(job.get(GenomixJobConf.OLD_STYLE));
            MIN_CONTIG_LENGTH = Integer.parseInt(job.get(GenomixJobConf.MIN_CONTIG_LENGTH));
            EXPECTED_GENOME_SIZE = Integer.parseInt(job.get(GenomixJobConf.EXPECTED_GENOME_SIZE));

            contigLengthList = new ArrayList<Integer>();
            maxContig = Integer.MIN_VALUE;
            minContig = Integer.MAX_VALUE;
            totalContigLength = 0;
            contigCount = 0;
            n10 = 0;
            n25 = 0;
            n50 = 0;
            n75 = 0;
            n95 = 0;
            n10count = 0;
            n25count = 0;
            n50count = 0;
            n75count = 0;
            n95count = 0;
            totalOverLength = 0;
            totalBPOverLength = 0;
            fSize = 0;

            nullWritable = NullWritable.get();
            outKey = new Text();
            tempKey = new StringBuffer();
        }

        @Override
        public void reduce(IntPair key, Iterator<IntWritable> values, OutputCollector<Text, NullWritable> output,
                Reporter reporter) throws IOException {
            while (values.hasNext())
                contigLengthList.add(values.next().get());

            for (Integer curLength : contigLengthList) {
                if (curLength <= MIN_CONTIG_LENGTH) {
                    continue;
                }

                if (OLD_STYLE == true && curLength <= MIN_CONTIG_LENGTH) {
                    continue;
                }

                if (curLength > maxContig) {
                    maxContig = curLength;
                }

                if (curLength < minContig) {
                    minContig = curLength;
                }
                if (EXPECTED_GENOME_SIZE == 0) {
                    totalContigLength += curLength;
                } else {
                    totalContigLength = EXPECTED_GENOME_SIZE;
                }
                contigCount++;

                // compute the E-size
                fSize += Math.pow(curLength, 2);
                if (curLength > MIN_CONTIG_LENGTH) {
                    totalOverLength++; // just like a count
                    totalBPOverLength += curLength;
                }
            }
            fSize /= EXPECTED_GENOME_SIZE;

            /*----------------------------------------------------------------*/
            // get the goal contig at X bases (1MBp, 2MBp)
            ArrayList<ContigAt> contigAtArray = new ArrayList<ContigAt>();
            if (USE_BAYLOR_FORMAT == true) {
                contigAtArray.add(new ContigAt(1 * CONTIG_AT_INITIAL_STEP));
                contigAtArray.add(new ContigAt(2 * CONTIG_AT_INITIAL_STEP));
                contigAtArray.add(new ContigAt(5 * CONTIG_AT_INITIAL_STEP));
                contigAtArray.add(new ContigAt(10 * CONTIG_AT_INITIAL_STEP));
            } else {
                long step = CONTIG_AT_INITIAL_STEP;
                long currentBases = 0;
                while (currentBases <= totalContigLength) {
                    if ((currentBases / step) >= 10) {
                        step *= 10;
                    }
                    currentBases += step;
                    contigAtArray.add(new ContigAt(currentBases));//
                }
            }
            ContigAt[] contigAtVals = contigAtArray.toArray(new ContigAt[0]);
            /*----------------------------------------------------------------*/
            //            Collections.sort(contigLengthList);

            long sum = 0;
            double median = 0;
            int medianCount = 0;
            int numberContigsSeen = 1;
            int currentValPoint = 0;

            for (int i = contigLengthList.size() - 1; i >= 0; i--) {
                if (((int) (contigCount / 2)) == i) {
                    median += contigLengthList.get(i);
                    medianCount++;
                } else if (contigCount % 2 == 0 && ((((int) (contigCount / 2)) + 1) == i)) {
                    median += contigLengthList.get(i);
                    medianCount++;
                }

                sum += contigLengthList.get(i);

                // calculate the bases at
                /*----------------------------------------------------------------*/
                while (currentValPoint < contigAtVals.length && sum >= contigAtVals[currentValPoint].goal
                        && contigAtVals[currentValPoint].count == 0) {
                    System.err.println("Calculating point at " + currentValPoint + " and the sum is " + sum
                            + " and i is" + i + " and lens is " + contigLengthList.size() + " and length is "
                            + contigLengthList.get(i));
                    contigAtVals[currentValPoint].count = numberContigsSeen;
                    contigAtVals[currentValPoint].len = contigLengthList.get(i);
                    contigAtVals[currentValPoint].totalBP = sum;
                    currentValPoint++;
                }
                /*----------------------------------------------------------------*/
                // calculate the NXs
                if (sum / (double) totalContigLength >= 0.1 && n10count == 0) {
                    n10 = contigLengthList.get(i);
                    n10count = contigLengthList.size() - i;
                }
                if (sum / (double) totalContigLength >= 0.25 && n25count == 0) {
                    n25 = contigLengthList.get(i);
                    n25count = contigLengthList.size() - i;
                }
                if (sum / (double) totalContigLength >= 0.5 && n50count == 0) {
                    n50 = contigLengthList.get(i);
                    n50count = contigLengthList.size() - i;
                }
                if (sum / (double) totalContigLength >= 0.75 && n75count == 0) {
                    n75 = contigLengthList.get(i);
                    n75count = contigLengthList.size() - i;
                }
                if (sum / (double) totalContigLength >= 0.95 && n95count == 0) {
                    n95 = contigLengthList.get(i);
                    n95count = contigLengthList.size() - i;
                }
                numberContigsSeen++;
            }

            if (OLD_STYLE == true) {
                tempKey.append("Total units: " + contigCount + "\n");
                tempKey.append("Reference: " + totalContigLength + "\n");
                tempKey.append("BasesInFasta: " + totalBPOverLength + "\n");
                tempKey.append("Min: " + minContig + "\n");
                tempKey.append("Max: " + maxContig + "\n");
                tempKey.append("N10: " + n10 + " COUNT: " + n10count + "\n");
                tempKey.append("N25: " + n25 + " COUNT: " + n25count + "\n");
                tempKey.append("N50: " + n50 + " COUNT: " + n50count + "\n");
                tempKey.append("N75: " + n75 + " COUNT: " + n75count + "\n");
                tempKey.append("E-size:" + nf.format(fSize) + "\n");
            } else {
                //                if (outputHeader) {
                tempKey.append("Assembly");
                tempKey.append(",Unit Number");
                tempKey.append(",Unit Total BP");
                tempKey.append(",Number Units > " + MIN_CONTIG_LENGTH);
                tempKey.append(",Total BP in Units > " + MIN_CONTIG_LENGTH);
                tempKey.append(",Min");
                tempKey.append(",Max");
                tempKey.append(",Average");
                tempKey.append(",Median");

                for (int i = 0; i < contigAtVals.length; i++) {
                    if (contigAtVals[i].count != 0) {
                        tempKey.append(",Unit At " + nf.format(contigAtVals[i].goal) + " Unit Count,"
                                + /*"Total Length," + */" Actual Unit Length");
                    }
                }
                tempKey.append("\n");
                //            }

                //            tempKey.append(title);
                tempKey.append("," + nf.format(contigCount));
                tempKey.append("," + nf.format(totalContigLength));
                tempKey.append("," + nf.format(totalOverLength));
                tempKey.append("," + nf.format(totalBPOverLength));
                tempKey.append("," + nf.format(minContig));
                tempKey.append("," + nf.format(maxContig));
                tempKey.append("," + nf.format((double) totalContigLength / contigCount));
                tempKey.append("," + nf.format((double) median / medianCount));

                for (int i = 0; i < contigAtVals.length; i++) {
                    if (contigAtVals[i].count != 0) {
                        tempKey.append("," + contigAtVals[i].count + ","
                                + /*contigAtVals[i].totalBP + "," +*/contigAtVals[i].len);
                    }
                }
            }
            outKey.set(tempKey.toString());
            output.collect(outKey, nullWritable);
        }
    }

    @SuppressWarnings("deprecation")
    public void run(String inputPath, String outputPath, int numReducers, JobConf baseConf) throws IOException {
        JobConf conf = new JobConf(baseConf);
        conf.setJarByClass(GetFastaStatsTest.class);
        conf.setJobName("GetFastaStats" + inputPath);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setMapOutputKeyClass(IntPair.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);

        conf.setMapperClass(GetFastaStatsMapper.class);
        conf.setReducerClass(GetFastaStatsReducer.class);

        conf.setPartitionerClass(IntPair.FirstPartitioner.class);
        conf.setOutputKeyComparatorClass(IntPair.Comparator.class);
        conf.setOutputValueGroupingComparator(IntPair.FirstGroupingComparator.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setNumReduceTasks(numReducers);

        FileSystem.get(conf).delete(new Path(outputPath), true);
        JobClient.runJob(conf);
    }

}
