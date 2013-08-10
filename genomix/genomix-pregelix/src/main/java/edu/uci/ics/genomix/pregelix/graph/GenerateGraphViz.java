package edu.uci.ics.genomix.pregelix.graph;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Iterator;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;

public class GenerateGraphViz {

	/**
     * Construct a DOT graph in memory, convert it
     * to image and store the image in the file system.
     */
    public static void convertGraphBuildingOutputToGraphViz(String srcDir, String destDir) throws Exception {
        GraphViz gv = new GraphViz();
        gv.addln(gv.start_graph());
        
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.getLocal(conf);
        File srcPath = new File(srcDir);

        String outputNode = "";
        String outputEdge = "";
        for (File f : srcPath.listFiles((FilenameFilter) (new WildcardFileFilter("part*")))) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, new Path(f.getAbsolutePath()), conf);
            VKmerBytesWritable key = new VKmerBytesWritable();
            NodeWritable value = new NodeWritable();
            
            gv.addln("rankdir=LR\n");
            
            while (reader.next(key, value)) {
                outputNode = "";
                outputEdge = "";
                if (key == null) {
                    break;
                }
                outputNode += key.toString();
                /** convert edge to graph **/
                outputEdge = convertEdgeToGraph(outputNode, value);
                gv.addln(outputEdge);
                /** add readIdSet **/
                outputNode += " [shape=record, label = \"<f0> " + key.toString() 
                        + "\"]\n";
                gv.addln(outputNode);
            }
            reader.close();
        }
        
        gv.addln(gv.end_graph());
        System.out.println(gv.getDotSource());

        String type = "ps";
        File folder = new File(destDir);
        folder.mkdirs();
        File out = new File(destDir + "/result." + type); // Linux
        gv.writeGraphToFile(gv.getGraph(gv.getDotSource(), type), out);
    }
    
    public static void convertGraphCleanOutputToGraphViz(String srcDir, String destDir) throws Exception {
        GraphViz gv = new GraphViz();
        gv.addln(gv.start_graph());
        
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.getLocal(conf);
        File srcPath = new File(srcDir);

        String outputNode = "";
        String outputEdge = "";
        for (File f : srcPath.listFiles((FilenameFilter) (new WildcardFileFilter("part*")))) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, new Path(f.getAbsolutePath()), conf);
            VKmerBytesWritable key = new VKmerBytesWritable();
            VertexValueWritable value = new VertexValueWritable();
            
            gv.addln("rankdir=LR\n");
            
            while (reader.next(key, value)) {
                outputNode = "";
                outputEdge = "";
                if (key == null) {
                    break;
                }
                outputNode += key.toString();
                /** convert edge to graph **/
                outputEdge = convertEdgeToGraph(outputNode, value);
                gv.addln(outputEdge);
                /** add readIdSet **/
                outputNode += " [shape=record, label = \"<f0> " + key.toString() 
//                        + "|<f1> " + value.getNodeIdList().printReadIdSet()
                        + "\"]\n";
                gv.addln(outputNode);
            }
            reader.close();
        }
        
        gv.addln(gv.end_graph());
        System.out.println(gv.getDotSource());

        String type = "ps";
        File folder = new File(destDir);
        folder.mkdirs();
        File out = new File(destDir + "/result." + type); // Linux
        gv.writeGraphToFile(gv.getGraph(gv.getDotSource(), type), out);
    }
    
    /**
     * For graph building
     * @param outputNode
     * @param value
     * @return
     */
    public static String convertEdgeToGraph(String outputNode, NodeWritable value){
        String outputEdge = "";
        Iterator<VKmerBytesWritable> kmerIterator;
        kmerIterator = value.getEdgeList(DirectionFlag.DIR_FF).iterator();
        while(kmerIterator.hasNext()){
            VKmerBytesWritable edge = kmerIterator.next(); 
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"black\" label =\"FF\"]\n";
        }
        kmerIterator = value.getEdgeList(DirectionFlag.DIR_FR).iterator();
        while(kmerIterator.hasNext()){
            VKmerBytesWritable edge = kmerIterator.next();
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"blue\" label =\"FR\"]\n";
        }
        kmerIterator = value.getEdgeList(DirectionFlag.DIR_RF).iterator();
        while(kmerIterator.hasNext()){
            VKmerBytesWritable edge = kmerIterator.next();
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"green\" label =\"RF\"]\n";
        }
        kmerIterator = value.getEdgeList(DirectionFlag.DIR_RR).iterator();
        while(kmerIterator.hasNext()){
            VKmerBytesWritable edge = kmerIterator.next();
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"red\" label =\"RR\"]\n";
        }
        //TODO should output actualKmer instead of kmer
        if(outputEdge == "")
            outputEdge += outputNode;
        return outputEdge;
    }
    
    /**
     * For graph clean
     * @param outputNode
     * @param value
     * @return
     */
    public static String convertEdgeToGraph(String outputNode, VertexValueWritable value){
        String outputEdge = "";
        Iterator<VKmerBytesWritable> kmerIterator;
        kmerIterator = value.getFFList().iterator();
        while(kmerIterator.hasNext()){
            VKmerBytesWritable edge = kmerIterator.next(); 
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"black\" label =\"FF\"]\n";
        }
        kmerIterator = value.getFRList().iterator();
        while(kmerIterator.hasNext()){
            VKmerBytesWritable edge = kmerIterator.next();
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"blue\" label =\"FR\"]\n";
        }
        kmerIterator = value.getRFList().iterator();
        while(kmerIterator.hasNext()){
            VKmerBytesWritable edge = kmerIterator.next();
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"green\" label =\"RF\"]\n";
        }
        kmerIterator = value.getRRList().iterator();
        while(kmerIterator.hasNext()){
            VKmerBytesWritable edge = kmerIterator.next();
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"red\" label =\"RR\"]\n";
        }
        //TODO should output actualKmer instead of kmer
        if(outputEdge == "")
            outputEdge += outputNode;
        return outputEdge;
    }

    public static void main(String[] args) throws Exception {
        GenerateGraphViz.convertGraphCleanOutputToGraphViz("data/actual/bubbleadd/BubbleAddGraph/bin/5", "graphtest");
    }
}
