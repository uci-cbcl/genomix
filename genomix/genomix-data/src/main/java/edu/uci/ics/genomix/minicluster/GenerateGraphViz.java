package edu.uci.ics.genomix.minicluster;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Map.Entry;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.ReadIdListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

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
                String fillColor = "";
                if(value.isStartReadOrEndRead())
                     fillColor = "fillcolor=\"grey\", style=\"filled\",";
                outputNode += " [shape=record, " + fillColor + " label = \"<f0> " + key.toString() 
                        + "|<f1> " + value.getStartReads().printStartReadIdSet() 
                        + "|<f2> " + value.getEndReads().printEndReadIdSet()
                        + "|<f3> " + value.getAvgCoverage() + "\"]\n";
                gv.addln(outputNode);
            }
            reader.close();
        }
        
        gv.addln(gv.end_graph());
        System.out.println(gv.getDotSource());

        String type = "svg";
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
            VKmerBytesWritable key = (VKmerBytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            NodeWritable value = (NodeWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            
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
                String fillColor = "";
                if(value.isStartReadOrEndRead())
                     fillColor = "fillcolor=\"grey\", style=\"filled\",";
                outputNode += " [shape=record, " + fillColor + " label = \"<f0> " + key.toString() 
                        + "|<f1> " + value.getStartReads().printStartReadIdSet() 
                        + "|<f2> " + value.getEndReads().printEndReadIdSet()
                        + "|<f3> " + value.getAvgCoverage()
                        + "|<f4> " + value.getInternalKmer() + "\"]\n";
                gv.addln(outputNode);
            }
            reader.close();
        }
        
        gv.addln(gv.end_graph());
        System.out.println(gv.getDotSource());

        String type = "svg";
//        File folder = new File(destDir);
//        folder.mkdirs();
        File out = new File(destDir + "." + type); // Linux
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
        for (EDGETYPE et : EDGETYPE.values()) {
            for (Entry<VKmerBytesWritable, ReadIdListWritable> e : value.getEdgeList(et).entrySet()) {
                outputEdge += outputNode + " -> " + e.getKey().toString() + "[color = \"black\" label =\"" + et + ": " + e.getValue() + "\"]\n";
            }
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
