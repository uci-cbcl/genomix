package edu.uci.ics.genomix.pregelix.sequencefile;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class ConvertNodeToIdValue {

    public static void convert(Path inFile, Path outFile)
            throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.get(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, inFile, conf);
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf, outFile, PositionWritable.class,
                VertexValueWritable.class, CompressionType.NONE);
        NodeWritable node = new NodeWritable();
        NullWritable value = NullWritable.get();
        PositionWritable outputKey = new PositionWritable();
        VertexValueWritable outputValue = new VertexValueWritable();

        while(reader.next(node, value)) {
            System.out.println(node.getNodeID().toString());
            outputKey.set(node.getNodeID());
            outputValue.setFFList(node.getFFList());
            outputValue.setFRList(node.getFRList());
            outputValue.setRFList(node.getRFList());
            outputValue.setRRList(node.getRRList());
            outputValue.setMergeChain(node.getKmer());
            outputValue.setState(State.NON_VERTEX);
            writer.append(outputKey, outputValue);
        }
        writer.close();
        reader.close();
    }
    
    public static void main(String[] args) throws IOException {
        Path dir = new Path("data/test");
        Path outDir = new Path("data/input");
        FileUtils.cleanDirectory(new File("data/input"));
        Path inFile = new Path(dir, "result.graphbuild.txt.bin");
        Path outFile = new Path(outDir, "out");
        convert(inFile,outFile);
    }
}
