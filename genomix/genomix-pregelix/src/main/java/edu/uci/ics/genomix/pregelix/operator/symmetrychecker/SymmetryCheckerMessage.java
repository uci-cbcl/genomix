package edu.uci.ics.genomix.pregelix.operator.symmetrychecker;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;

public class SymmetryCheckerMessage extends MessageWritable {

    protected class SYMMERTRYCHECKER_MESSAGE_FIELDS extends MESSAGE_FIELDS {
        public static final byte EDGE_MAP = 1 << 1; // used in subclass: SymmetryCheckerMessage
    }

    private VKmerList edges;

    public SymmetryCheckerMessage() {
        super();
        edges = null;
    }

    @Override
    public void reset() {
        super.reset();
        edges = null;
    }

    public VKmerList getEdges() {
        if (edges == null) {
            edges = new VKmerList();
        }
        return edges;
    }

    public void setEdges(VKmerList otherEdges) {
        getEdges().clear();
        getEdges().appendList(otherEdges);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((messageFields & SYMMERTRYCHECKER_MESSAGE_FIELDS.EDGE_MAP) != 0) {
            getEdges().readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (edges != null) {
            edges.write(out);
        }
    }

    @Override
    protected byte getActiveMessageFields() {
        byte messageFields = super.getActiveMessageFields();
        if (edges != null) {
            messageFields |= SYMMERTRYCHECKER_MESSAGE_FIELDS.EDGE_MAP;
        }
        return messageFields;
    }
}
