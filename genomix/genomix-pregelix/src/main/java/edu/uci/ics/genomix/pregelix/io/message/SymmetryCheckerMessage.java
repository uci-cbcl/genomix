package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.EdgeMap;

public class SymmetryCheckerMessage extends MessageWritable {

    private EdgeMap edgeMap;

    public SymmetryCheckerMessage() {
        super();
        edgeMap = new EdgeMap();
    }
    
    public void reset(){
        super.reset();
        edgeMap.clear();
    }
    
    public EdgeMap getEdgeMap() {
        return edgeMap;
    }

    public void setEdgeMap(EdgeMap edgeMap) {
        this.edgeMap.clear();
        this.edgeMap.putAll(edgeMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        edgeMap.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        edgeMap.write(out);
    }
}
