package edu.uci.ics.genomix.hadoop.tp.graphclean.graphviz;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.types.Pair;

import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeMsgWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeNode;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanDestId;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanGenericValue;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.MsgWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;

public class GenerateGraphViz {

    /**
     * Construct a DOT graph in memory, convert it
     * to image and store the image in the file system.
     */
    public static void convertGraphBuildingOutputToGraphViz(List<Pair<GraphCleanDestId, GraphCleanGenericValue>> input,
            String destDir) throws Exception {
        GraphViz gv = new GraphViz();
        gv.addln(gv.start_graph());

        String outputNode = "";
        String outputEdge = "";

        GraphCleanDestId key = new GraphCleanDestId();
        GraphCleanGenericValue value = new GraphCleanGenericValue();

        PathMergeNode curNode = new PathMergeNode();
        PathMergeMsgWritable incomingMsg = new PathMergeMsgWritable();

        gv.addln("rankdir=LR\n");

        for (Pair<GraphCleanDestId, GraphCleanGenericValue> iter : input) {
            outputNode = "";
            outputEdge = "";

            key.setAsCopy(iter.getFirst());
            value = iter.getSecond();
            Writable tempValue = value.get();

            outputNode += key.toString();
            if (tempValue instanceof PathMergeNode) {
                curNode.setAsCopy((NodeWritable) tempValue);
                outputEdge = convertEdgeToGraph(outputNode, key, value);
                gv.addln(outputEdge);
                /** add readIdSet **/
                String fillColor = "";
                if (curNode.isStartReadOrEndRead())
                    fillColor = "fillcolor=\"grey\", style=\"filled\",";
                outputNode += " [shape=record, " + fillColor + " label = \"<f0> " + key.toString() + "|<f1> "
                        + curNode.getStartReads().printStartReadIdSet() + "|<f2> "
                        + curNode.getEndReads().printEndReadIdSet() + "|<f3> " + curNode.getAvgCoverage() + "\"]\n";
                gv.addln(outputNode);
            } else {
                incomingMsg.setAsCopy((PathMergeMsgWritable) value.get());
                outputEdge = convertEdgeToGraph(outputNode, key, value);
                gv.addln(outputEdge);
            }
        }

        /** convert edge to graph **/
        gv.addln(gv.end_graph());
        System.out.println(gv.getDotSource());

        String type = "png";
        File folder = new File(destDir);
        folder.mkdirs();
        File out = new File(destDir + "/result." + type); // Linux
        gv.writeGraphToFile(gv.getGraph(gv.getDotSource(), type), out);
    }

    public static String convertEdgeToGraph(String outputNode, GraphCleanDestId key, GraphCleanGenericValue value) {
        PathMergeNode curNode = new PathMergeNode();
        PathMergeMsgWritable incomingMsg = new PathMergeMsgWritable();
        Writable tempValue = value.get();
        String outputEdge = "";
        if (tempValue instanceof PathMergeNode) {
            curNode.setAsCopy((NodeWritable) tempValue);

            Iterator<EdgeWritable> edgeIterator;
            edgeIterator = curNode.getEdgeList(EDGETYPE.FF).iterator();
            while (edgeIterator.hasNext()) {
                EdgeWritable edge = edgeIterator.next();
                outputEdge += outputNode + " -> " + edge.getKey().toString() + "[color = \"black\" label =\"FF: "
                        + edge.printReadIdSet() + "\"]\n";
            }
            edgeIterator = curNode.getEdgeList(EDGETYPE.FR).iterator();
            while (edgeIterator.hasNext()) {
                EdgeWritable edge = edgeIterator.next();
                outputEdge += outputNode + " -> " + edge.getKey().toString() + "[color = \"black\" label =\"FR: "
                        + edge.printReadIdSet() + "\"]\n";
            }
            edgeIterator = curNode.getEdgeList(EDGETYPE.RF).iterator();
            while (edgeIterator.hasNext()) {
                EdgeWritable edge = edgeIterator.next();
                outputEdge += outputNode + " -> " + edge.getKey().toString() + "[color = \"black\" label =\"RF: "
                        + edge.printReadIdSet() + "\"]\n";
            }
            edgeIterator = curNode.getEdgeList(EDGETYPE.RR).iterator();
            while (edgeIterator.hasNext()) {
                EdgeWritable edge = edgeIterator.next();
                outputEdge += outputNode + " -> " + edge.getKey().toString() + "[color = \"black\" label =\"RR: "
                        + edge.printReadIdSet() + "\"]\n";
            }
        } else {
            incomingMsg.setAsCopy((PathMergeMsgWritable) value.get());
            outputEdge += incomingMsg.getSourceVertexId().toString() + " -> " + key.toString()
                    + "[color = \"red\" label =\"message" + "\"]\n";
        }
        //TODO should output actualKmer instead of kmer
        if (outputEdge == "")
            outputEdge += outputNode;
        return outputEdge;
    }
}
