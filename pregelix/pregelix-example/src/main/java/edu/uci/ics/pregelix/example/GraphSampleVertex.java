package edu.uci.ics.pregelix.example;

import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.example.io.DoubleWritable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

public class GraphSampleVertex extends Vertex<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    public static final String SMAPLING_RATE = "pregelix.samplingrate";
    
    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) throws Exception {
        
    }

}
