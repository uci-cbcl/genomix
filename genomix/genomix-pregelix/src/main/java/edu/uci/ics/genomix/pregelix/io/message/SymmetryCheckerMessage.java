package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.EdgeMap;

public class SymmetryCheckerMessage extends MessageWritable {

    protected class SYMMERTRYCHECKER_MESSAGE_FIELDS extends MESSAGE_FIELDS {
        public static final byte EDGE_MAP = 1 << 1; // used in subclass: SymmetryCheckerMessage
    }

    private EdgeMap edgeMap;

    public SymmetryCheckerMessage() {
        super();
        edgeMap = null;
    }

    @Override
    public void reset() {
        super.reset();
        edgeMap = null;
    }

    public EdgeMap getEdgeMap() {
        if (edgeMap == null) {
            edgeMap = new EdgeMap();
        }
        return edgeMap;
    }

    public void setEdgeMap(EdgeMap edgeMap) {
        getEdgeMap().clear();
        getEdgeMap().putAll(edgeMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((messageFields & SYMMERTRYCHECKER_MESSAGE_FIELDS.EDGE_MAP) != 0) {
            getEdgeMap().readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (edgeMap != null) {
            edgeMap.write(out);
        }
    }

    @Override
    protected byte getActiveMessageFields() {
        byte messageFields = super.getActiveMessageFields();
        if (edgeMap != null) {
            messageFields |= SYMMERTRYCHECKER_MESSAGE_FIELDS.EDGE_MAP;
        }
        return messageFields;
    }
}
