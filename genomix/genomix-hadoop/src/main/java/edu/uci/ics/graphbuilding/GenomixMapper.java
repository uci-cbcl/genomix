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

//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class implement mapper operator of mapreduce model
 */
@SuppressWarnings("deprecation")
public class GenomixMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, ValueBytesWritable, ValueWritable> {

    public class CurrenByte {
        public byte curByte;
        public byte preMarker;
    }

    public static int KMER_SIZE;
    public ValueWritable outputAdjList = new ValueWritable();
    public ValueBytesWritable outputKmer = new ValueBytesWritable();

    @Override
    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
    }

    public CurrenByte shift(byte curByte, byte newKmer) {
        CurrenByte currentByte = new CurrenByte();
        byte preMarker = (byte) 0xC0;
        preMarker = (byte) (preMarker & curByte);
        curByte = (byte) (curByte << 2);
        curByte = (byte) (curByte | newKmer);
        preMarker = (byte) ((preMarker & 0xff) >> 6);
        currentByte.curByte = curByte;
        currentByte.preMarker = preMarker;
        return currentByte;
    }

    public CurrenByte lastByteShift(byte curByte, byte newKmer, int kmerSize) {
        CurrenByte currentByte = new CurrenByte();
        int restBits = (kmerSize * 2) % 8;
        if (restBits == 0)
            restBits = 8;
        byte preMarker = (byte) 0x03;
        preMarker = (byte) (preMarker << restBits - 2);
        preMarker = (byte) (preMarker & curByte);
        preMarker = (byte) ((preMarker & 0xff) >> restBits - 2);
        byte reset = 3;
        reset = (byte) ~(reset << restBits - 2);
        curByte = (byte) (curByte & reset);
        curByte = (byte) (curByte << 2);
        curByte = (byte) (curByte | newKmer);
        currentByte.curByte = curByte;
        currentByte.preMarker = preMarker;
        return currentByte;
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
    public void map(LongWritable key, Text value, OutputCollector<ValueBytesWritable, ValueWritable> output,
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
            int size = 0;
            if (KMER_SIZE * 2 % 8 == 0)
                size = KMER_SIZE * 2 / 8;
            else
                size = KMER_SIZE * 2 / 8 + 1;
            byte[] kmerValue = new byte[size];
            for (int k = 0; k < kmerValue.length; k++)
                kmerValue[i] = 0x00;
            CurrenByte currentByte = new CurrenByte();
            byte preMarker = (byte) -1;
            byte count = 0;
            //Get the next kmer by shifting one letter every time
            for (i = 0; i < geneLine.length(); i++) {
                byte kmerAdjList = 0;
                byte initial;
                if (i >= KMER_SIZE) {
                    outputKmer.set(kmerValue, (byte) 0, (byte) size);
                    switch ((int) preMarker) {
                        case -1:
                            kmerAdjList = (byte) (kmerAdjList + 0);
                            break;
                        case 0:
                            kmerAdjList = (byte) (kmerAdjList + 16);
                            break;
                        case 1:
                            kmerAdjList = (byte) (kmerAdjList + 32);
                            break;
                        case 2:
                            kmerAdjList = (byte) (kmerAdjList + 64);
                            break;
                        case 3:
                            kmerAdjList = (byte) (kmerAdjList + 128);
                            break;
                    }
                }
                switch (geneLine.charAt(i)) {
                    case 'A':
                        kmerAdjList = (byte) (kmerAdjList + 1);
                        initial = (byte) 0x00;
                        if (kmerValue.length == 1) {
                            currentByte = lastByteShift(kmerValue[kmerValue.length - 1], initial, KMER_SIZE);
                            preMarker = currentByte.preMarker;
                            kmerValue[kmerValue.length - 1] = currentByte.curByte;
                        } else {
                            currentByte = shift(kmerValue[0], initial);
                            preMarker = currentByte.preMarker;
                            kmerValue[0] = currentByte.curByte;
                            for (int j = 1; j < kmerValue.length - 1; j++) {
                                currentByte = shift(kmerValue[j], preMarker);
                                preMarker = currentByte.preMarker;
                                kmerValue[j] = currentByte.curByte;
                            }
                            currentByte = lastByteShift(kmerValue[kmerValue.length - 1], preMarker, KMER_SIZE);
                            preMarker = currentByte.preMarker;
                            kmerValue[kmerValue.length - 1] = currentByte.curByte;
                        }

                        break;
                    case 'G':
                        kmerAdjList = (byte) (kmerAdjList + 2);
                        initial = (byte) 0x01;
                        if (kmerValue.length == 1) {
                            currentByte = lastByteShift(kmerValue[kmerValue.length - 1], initial, KMER_SIZE);
                            preMarker = currentByte.preMarker;
                            kmerValue[kmerValue.length - 1] = currentByte.curByte;
                        } else {
                            currentByte = shift(kmerValue[0], initial);
                            preMarker = currentByte.preMarker;
                            kmerValue[0] = currentByte.curByte;
                            for (int j = 1; j < kmerValue.length - 1; j++) {
                                currentByte = shift(kmerValue[j], preMarker);
                                preMarker = currentByte.preMarker;
                                kmerValue[j] = currentByte.curByte;
                            }
                            currentByte = lastByteShift(kmerValue[kmerValue.length - 1], preMarker, KMER_SIZE);
                            preMarker = currentByte.preMarker;
                            kmerValue[kmerValue.length - 1] = currentByte.curByte;
                        }
                        break;
                    case 'C':
                        kmerAdjList = (byte) (kmerAdjList + 4);
                        initial = (byte) 0x02;
                        if (kmerValue.length == 1) {
                            currentByte = lastByteShift(kmerValue[kmerValue.length - 1], initial, KMER_SIZE);
                            preMarker = currentByte.preMarker;
                            kmerValue[kmerValue.length - 1] = currentByte.curByte;
                        } else {
                            currentByte = shift(kmerValue[0], initial);
                            preMarker = currentByte.preMarker;
                            kmerValue[0] = currentByte.curByte;
                            for (int j = 1; j < kmerValue.length - 1; j++) {
                                currentByte = shift(kmerValue[j], preMarker);
                                preMarker = currentByte.preMarker;
                                kmerValue[j] = currentByte.curByte;
                            }
                            currentByte = lastByteShift(kmerValue[kmerValue.length - 1], preMarker, KMER_SIZE);
                            preMarker = currentByte.preMarker;
                            kmerValue[kmerValue.length - 1] = currentByte.curByte;
                        }
                        break;
                    case 'T':
                        kmerAdjList = (byte) (kmerAdjList + 8);
                        initial = (byte) 0x03;
                        if (kmerValue.length == 1) {
                            currentByte = lastByteShift(kmerValue[kmerValue.length - 1], initial, KMER_SIZE);
                            preMarker = currentByte.preMarker;
                            kmerValue[kmerValue.length - 1] = currentByte.curByte;
                        } else {
                            currentByte = shift(kmerValue[0], initial);
                            preMarker = currentByte.preMarker;
                            kmerValue[0] = currentByte.curByte;
                            for (int j = 1; j < kmerValue.length - 1; j++) {
                                currentByte = shift(kmerValue[j], preMarker);
                                preMarker = currentByte.preMarker;
                                kmerValue[j] = currentByte.curByte;
                            }
                            currentByte = lastByteShift(kmerValue[kmerValue.length - 1], preMarker, KMER_SIZE);
                            preMarker = currentByte.preMarker;
                            kmerValue[kmerValue.length - 1] = currentByte.curByte;
                        }
                        break;
                }
                if (i >= KMER_SIZE) {
                    outputAdjList.set(kmerAdjList, count);
                    output.collect(outputKmer, outputAdjList);
                }
                if (i < KMER_SIZE)
                    preMarker = (byte) -1;
            }
            // arrive the last letter of this gene line
            if (i == geneLine.length()) {
                byte kmerAdjList = 0;
                switch ((int) preMarker) {
                    case -1:
                        kmerAdjList = (byte) (kmerAdjList + 0);
                        break;
                    case 0:
                        kmerAdjList = (byte) (kmerAdjList + 16);
                        break;
                    case 1:
                        kmerAdjList = (byte) (kmerAdjList + 32);
                        break;
                    case 2:
                        kmerAdjList = (byte) (kmerAdjList + 64);
                        break;
                    case 3:
                        kmerAdjList = (byte) (kmerAdjList + 128);
                        break;
                }
                outputAdjList.set(kmerAdjList, count);
                outputKmer.set(kmerValue, (byte) 0, (byte) size);
                output.collect(outputKmer, outputAdjList);
            }
        }
    }
}