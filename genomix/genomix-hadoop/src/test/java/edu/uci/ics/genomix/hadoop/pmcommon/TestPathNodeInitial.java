package edu.uci.ics.genomix.hadoop.pmcommon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Test;

import edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3.MergePathsH3.MessageFlag;
import edu.uci.ics.genomix.hadoop.pmcommon.MessageWritableNodeWithFlag;
import edu.uci.ics.genomix.hadoop.pmcommon.PathNodeInitial;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class TestPathNodeInitial {
    PositionWritable posn1 = new PositionWritable(0, (byte) 1);
    PositionWritable posn2 = new PositionWritable(1, (byte) 1);
    PositionWritable posn3 = new PositionWritable(2, (byte) 1);
    PositionWritable posn4 = new PositionWritable(3, (byte) 1);
    PositionWritable posn5 = new PositionWritable(5, (byte) 1);
    String kmerString = "ATGCA";
    KmerBytesWritable kmer = new KmerBytesWritable(kmerString.length(), kmerString);
    JobConf conf = new JobConf();
    MultipleOutputs mos = new MultipleOutputs(conf); 

    {
        conf.set("sizeKmer", String.valueOf(kmerString.length()));
    }

    @Test
    public void testNoNeighbors() throws IOException {
        NodeWritable noNeighborNode = new NodeWritable(posn1, new PositionListWritable(), new PositionListWritable(),
                new PositionListWritable(), new PositionListWritable(), kmer);
        MessageWritableNodeWithFlag output = new MessageWritableNodeWithFlag((byte) (MessageFlag.FROM_SELF | MessageFlag.IS_COMPLETE), noNeighborNode);
        // test mapper
        new MapDriver<NodeWritable, NullWritable, PositionWritable, MessageWritableNodeWithFlag>()
                .withMapper(new PathNodeInitial.PathNodeInitialMapper())
                .withConfiguration(conf)
                .withInput(noNeighborNode, NullWritable.get())
                .withOutput(posn1, output)
                .runTest();
        // test reducer
//        MultipleOutputs.addNamedOutput(conf, "complete", SequenceFileOutputFormat.class, PositionWritable.class, MessageWritableNodeWithFlag.class);
        new ReduceDriver<PositionWritable, MessageWritableNodeWithFlag, PositionWritable, MessageWritableNodeWithFlag>()
                .withReducer(new PathNodeInitial.PathNodeInitialReducer())
                .withConfiguration(conf)
                .withInput(posn1, Arrays.asList(output))
                .runTest();
    }
}
