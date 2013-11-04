package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.EdgeMap;

public class SymmetryCheckerMessage extends MessageWritable {
    
    class SYMMERTRYCHECKER_MESSAGE_FIELDS extends MESSAGE_FIELDS{
        public static final byte EDGE_MAP = 1 << 1; // used in subclass: SymmetryCheckerMessage
    }
    
    private EdgeMap edgeMap;

    public SymmetryCheckerMessage() {
        super();
        edgeMap = new EdgeMap();
    }

    public void reset() {
        super.reset();
        edgeMap.clear();
    }

    public EdgeMap getEdgeMap() {
        return edgeMap;
    }

    public void setEdgeMap(EdgeMap edgeMap) {
        validMessageFlag |= SYMMERTRYCHECKER_MESSAGE_FIELDS.EDGE_MAP;
        this.edgeMap.clear();
        this.edgeMap.putAll(edgeMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        if ((validMessageFlag & SYMMERTRYCHECKER_MESSAGE_FIELDS.EDGE_MAP) > 0)
            edgeMap.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if ((validMessageFlag & SYMMERTRYCHECKER_MESSAGE_FIELDS.EDGE_MAP) > 0)
            edgeMap.write(out);
    }
}
