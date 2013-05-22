package edu.uci.ics.genomix.type;

public class NoteWritableFactory {
    private NodeWritable node;
    private KmerBytesWritableFactory kmerBytesWritableFactory;
    private int kmerSize = 55;

    public NoteWritableFactory() {
        node = new NodeWritable();
        kmerBytesWritableFactory = new KmerBytesWritableFactory(kmerSize);
    }

    public NodeWritable append(final NodeWritable orignalNode, final KmerBytesWritable appendKmer){
        KmerBytesWritable preKmer = orignalNode.getKmer();
        node.setKmer(kmerBytesWritableFactory.mergeTwoKmer(preKmer,appendKmer));
        return node;
    }
    
    public NodeWritable append(final NodeWritable orignalNode, final NodeWritable appendNode) {
        KmerBytesWritable nextKmer = kmerBytesWritableFactory.getSubKmerFromChain(kmerSize - 2, appendNode.getKmer().kmerlength - kmerSize + 2,
                appendNode.getKmer());
        return append(orignalNode, nextKmer);
    }

    public NodeWritable prepend(final NodeWritable orignalNode, final KmerBytesWritable prependKmer){
        KmerBytesWritable nextKmer = orignalNode.getKmer();
        node.setKmer(kmerBytesWritableFactory.mergeTwoKmer(prependKmer,nextKmer));
        return node;
    }
    
    public NodeWritable prepend(final NodeWritable orignalNode, final NodeWritable prependNode) {
        KmerBytesWritable prependKmer = kmerBytesWritableFactory.getSubKmerFromChain(kmerSize - 2, orignalNode.getKmer().kmerlength - kmerSize + 2,
                orignalNode.getKmer());
        return prepend(orignalNode, prependKmer);
    }

}
