package edu.uci.ics.genomix.hadoop.valvetgraphbuilding;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;

@SuppressWarnings("deprecation")
public class DeepGraphBuildingReducer extends MapReduceBase implements
        Reducer<IntWritable, LineBasedmappingWritable, NodeWritable, NullWritable> {

/*    public ArrayList<LineBasedmappingWritable> lineElementsSet = new ArrayList<LineBasedmappingWritable>();
    public Position outputVerID = new Position();
    public VertexAdjacentWritable outputAdjacentList = new VertexAdjacentWritable();
    public PositionList srcVtexAdjList = new PositionList();
    public PositionList desVtexAdjList = new PositionList();
    public VertexIDListWritable srcAdjListWritable = new VertexIDListWritable();
    public VKmerBytesWritable desKmer = new VKmerBytesWritable(1);
    public VKmerBytesWritableFactory kmerFactory = new VKmerBytesWritableFactory(1);
    public VKmerBytesWritable srcKmer = new VKmerBytesWritable(1);*/
    @Override
    public void reduce(IntWritable key, Iterator<LineBasedmappingWritable> values,
            OutputCollector<NodeWritable, NullWritable> output, Reporter reporter) throws IOException {
/*        while (values.hasNext()) {
            lineElementsSet.add(values.next());
        }
        int[] orderLineTable = new int[lineElementsSet.size()];
        for (int i = 0; i < lineElementsSet.size(); i++) {
            int posInInvertedIndex = lineElementsSet.get(i).getPosInInvertedIndex();
            orderLineTable[lineElementsSet.get(i).getAdjVertexList().get().getPosinReadListElement(posInInvertedIndex)] = i;
        }
        //the first node in this read
        int posInInvertedIndex = lineElementsSet.get(orderLineTable[0]).getPosInInvertedIndex();
        outputVerID.set(
                lineElementsSet.get(orderLineTable[0]).getAdjVertexList().get().getReadListElement(posInInvertedIndex),
                (byte) 0);
        desVtexAdjList.set(lineElementsSet.get(orderLineTable[1]).getAdjVertexList().get());
        for (int i = 0; i < desVtexAdjList.getUsedSize(); i++) {
            if (desVtexAdjList.getPosinReadListElement(i) == (byte) 0) {
                srcVtexAdjList.addELementToList(desVtexAdjList.getReadListElement(i), (byte) 0);
            }
        }
        srcVtexAdjList.addELementToList(key.get(), (byte) 1);
        outputVerID.set(
                lineElementsSet.get(orderLineTable[0]).getAdjVertexList().get().getReadListElement(posInInvertedIndex),
                (byte) 0);
        srcAdjListWritable.set(srcVtexAdjList);
        outputAdjacentList.set(srcAdjListWritable, lineElementsSet.get(orderLineTable[0]).getVkmer());
        output.collect(outputVerID, outputAdjacentList);
        //srcVtexAdjList reset!!!!

        for (int i = 1; i < lineElementsSet.size(); i++) {
            desVtexAdjList.set(lineElementsSet.get(orderLineTable[i + 1]).getAdjVertexList().get());
            boolean flag = false;
            for (int j = 0; j < desVtexAdjList.getUsedSize(); j++) {
                if (desVtexAdjList.getPosinReadListElement(j) == (byte) 0) {
                    srcVtexAdjList.addELementToList(desVtexAdjList.getReadListElement(i), (byte) 0);
                    flag = true;
                }
            }
            if (flag = true) {
                //doesm't merge
                srcVtexAdjList.addELementToList(key.get(), (byte) (i + 1));
                outputVerID.set(
                        lineElementsSet.get(orderLineTable[i]).getAdjVertexList().get()
                                .getReadListElement(posInInvertedIndex), lineElementsSet.get(orderLineTable[i])
                                .getAdjVertexList().get().getPosinReadListElement(posInInvertedIndex));
                srcAdjListWritable.set(srcVtexAdjList);
                outputAdjacentList.set(srcAdjListWritable, lineElementsSet.get(orderLineTable[i]).getVkmer());
            }
            else {
                //merge
                desKmer.set(kmerFactory.getFirstKmerFromChain(1, lineElementsSet.get(orderLineTable[i+1]).getVkmer()));
                srcKmer.set(lineElementsSet.get(orderLineTable[i]).getVkmer());
                lineElementsSet.get(orderLineTable[i+1]).getVkmer().set(kmerFactory.mergeTwoKmer(srcKmer, desKmer));
                orderLineTable[i+1] = orderLineTable[i];
            }
        }*/
    }
}
