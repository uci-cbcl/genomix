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
package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.pmcommon.MergePathValueWritable;
import edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3.State;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

@SuppressWarnings("deprecation")
public class MergePathH3Mapper extends MapReduceBase implements
        Mapper<VKmerBytesWritable, MergePathValueWritable, VKmerBytesWritable, MergePathValueWritable> {

    private int KMER_SIZE;
    private VKmerBytesWritableFactory outputKmerFactory;
    private MergePathValueWritable outputValue;
    private VKmerBytesWritable tmpKmer;
    private VKmerBytesWritable outputKmer;

    private Random randGenerator;
    private float probBeingRandomHead;

    public void configure(JobConf job) {
        KMER_SIZE = job.getInt("sizeKmer", 0);
        outputKmerFactory = new VKmerBytesWritableFactory(KMER_SIZE);
        outputValue = new MergePathValueWritable();
        tmpKmer = new VKmerBytesWritable(KMER_SIZE);
        outputKmer = new VKmerBytesWritable(KMER_SIZE);

        randGenerator = new Random(job.getLong("randSeed", 0));
        probBeingRandomHead = job.getFloat("probBeingRandomHead", 0.5f);
    }

    private boolean isNodeRandomHead() {
        return randGenerator.nextFloat() < probBeingRandomHead;
    }

    /*
     * Retrieve the kmer corresponding to the single predecessor of kmerChain
     * Make sure there is really only one neighbor in adjBitMap
     */
    private KmerBytesWritable getPredecessorKmer(KmerBytesWritable kmerChain, byte adjBitMap) {
        byte preCode = (byte) ((adjBitMap & 0xF0) >> 4);
        preCode = GeneCode.getGeneCodeFromBitMap(preCode);
        return outputKmerFactory.shiftKmerWithPreCode(outputKmerFactory.getFirstKmerFromChain(KMER_SIZE, kmerChain),
                preCode);
    }

    @Override
    public void map(VKmerBytesWritable key, MergePathValueWritable value,
            OutputCollector<VKmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        byte adjBitMap = value.getAdjBitMap();

        // Map all path vertices; tail nodes are sent to their predecessors
        if (VertexUtil.isPathVertex(adjBitMap)) {
            if (isNodeRandomHead()) {
                // head nodes map themselves
                outputValue.set(adjBitMap, State.FROM_SELF, key);
                output.collect(key, outputValue);
            } else {
                // tail nodes send themselves to their predecessor
                outputKmer.set(getPredecessorKmer(key, adjBitMap));
                outputValue.set(adjBitMap, State.FROM_SUCCESSOR, key);
                output.collect(outputKmer, outputValue);
            }
        }
    }
}
