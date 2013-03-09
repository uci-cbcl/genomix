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

package edu.uci.ics.graphbuilding;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.Kmer.GENE_CODE;
import edu.uci.ics.genomix.type.KmerCountValue;

/**
 * This class implement mapper operator of mapreduce model
 */
@SuppressWarnings("deprecation")
public class GenomixMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, BytesWritable, KmerCountValue> {

    public class CurrenByte {
        public byte curByte;
        public byte preMarker;
    }

    public static int KMER_SIZE;
    public KmerCountValue outputAdjList = new KmerCountValue();
    public BytesWritable outputKmer = new BytesWritable();

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
    public void map(LongWritable key, Text value, OutputCollector<BytesWritable, KmerCountValue> output,
            Reporter reporter) throws IOException {
        /* A 00
           G 01
           C 10
           T 11*/
        String geneLine = value.toString(); // Read the Real Gene Line
        Pattern genePattern = Pattern.compile("[AGCT]+");
        Matcher geneMatcher = genePattern.matcher(geneLine);
        boolean isValid = geneMatcher.matches();
        if (isValid == true) {
            /** first kmer */
            byte count = 0;
            byte[] array = geneLine.getBytes();
            byte[] kmer = Kmer.CompressKmer(KMER_SIZE, array, 0);
            byte pre = 0;
            byte next = GENE_CODE.getAdjBit(array[KMER_SIZE]);
            byte adj = GENE_CODE.mergePreNextAdj(pre, next);
            outputAdjList.reset(adj, count);
            outputKmer.set(kmer, 0, kmer.length);
            output.collect(outputKmer, outputAdjList);
            /** middle kmer */
            for (int i = KMER_SIZE; i < array.length - 1; i++) {
                pre = Kmer.MoveKmer(KMER_SIZE, kmer, array[i]);
                next = GENE_CODE.getAdjBit(array[i + 1]);
                adj = GENE_CODE.mergePreNextAdj(pre, next);
                outputAdjList.reset(adj, count);
                outputKmer.set(kmer, 0, kmer.length);
                output.collect(outputKmer, outputAdjList);
            }
            /** last kmer */
            pre = Kmer.MoveKmer(KMER_SIZE, kmer, array[array.length - 1]);
            next = 0;
            adj = GENE_CODE.mergePreNextAdj(pre, next);
            outputAdjList.reset(adj, count);
            outputKmer.set(kmer, 0, kmer.length);
            output.collect(outputKmer, outputAdjList);
        }
    }
}