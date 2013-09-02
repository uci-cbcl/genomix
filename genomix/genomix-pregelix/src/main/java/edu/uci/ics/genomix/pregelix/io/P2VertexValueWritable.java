package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.pregelix.io.message.P2PathMergeMessageWritable.P2MessageType;
import edu.uci.ics.genomix.type.NodeWritable;

public class P2VertexValueWritable extends VertexValueWritable{
    
    private static final long serialVersionUID = -6600330062969997327L;
    
    private NodeWritable prependMergeNode;
    private NodeWritable appendMergeNode;
    
    public P2VertexValueWritable(){
        super();
        prependMergeNode = new NodeWritable();
        appendMergeNode = new NodeWritable();
    }
    
    public void reset(){
        super.reset();
        prependMergeNode.reset();
        appendMergeNode.reset();
    }
    
    public NodeWritable getMergeNode(byte mergeMsgType){
        switch(mergeMsgType){
            case P2MessageType.FROM_PREDECESSOR:
                return getPrependMergeNode();
            case P2MessageType.FROM_SUCCESSOR:
                return getAppendMergeNode();
        }
        return null;
    }
    
    /**
     * process finalNode 
     */
    public void processFinalNode(){
        String prepend = prependMergeNode.getInternalKmer().toString();
        String append = appendMergeNode.getInternalKmer().toString();
        append.substring(getNode().getInternalKmer().getKmerLetterLength());
        String merge = prepend + append;
        getNode().getInternalKmer().setByRead(merge.length(), merge.getBytes(), 0);
    }
    
    public NodeWritable getPrependMergeNode() {
        return prependMergeNode;
    }

    public void setPrependMergeNode(NodeWritable prependMergeNode) {
        this.prependMergeNode.setAsCopy(prependMergeNode);
    }

    public NodeWritable getAppendMergeNode() {
        return appendMergeNode;
    }

    public void setAppendMergeNode(NodeWritable appendMergeNode) {
        this.appendMergeNode.setAsCopy(appendMergeNode);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        prependMergeNode.readFields(in);
        appendMergeNode.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        prependMergeNode.write(out);
        appendMergeNode.write(out);
    }
}
