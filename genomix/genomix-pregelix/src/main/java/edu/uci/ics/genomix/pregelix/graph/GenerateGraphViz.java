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
import edu.uci.ics.genomix.type.KmerBytesWritable;

public class GenerateGraphViz {

	/**
     * Construct a DOT graph in memory, convert it
     * to image and store the image in the file system.
     */
    private void convertGraphCleanOutputToGraphViz(String srcDir, String destDir) throws Exception {
        GraphViz gv = new GraphViz();
        gv.addln(gv.start_graph());
        
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.getLocal(conf);
        File srcPath = new File(srcDir);

        String outputNode = "";
        String outputEdge = "";
        for (File f : srcPath.listFiles((FilenameFilter) (new WildcardFileFilter("part*")))) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, new Path(f.getAbsolutePath()), conf);
            KmerBytesWritable key = new KmerBytesWritable();
            VertexValueWritable value = new VertexValueWritable();
            
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
            }
            reader.close();
        }
        
        gv.addln(gv.end_graph());
        System.out.println(gv.getDotSource());

        String type = "ps";
        File out = new File("graphtest/" + "test_out." + type); // Linux
        gv.writeGraphToFile(gv.getGraph(gv.getDotSource(), type), out);
    }
    
    public String convertEdgeToGraph(String outputNode, VertexValueWritable value){
        String outputEdge = "";
        Iterator<KmerBytesWritable> kmerIterator;
        kmerIterator = value.getFFList().iterator();
        while(kmerIterator.hasNext()){
            KmerBytesWritable edge = kmerIterator.next(); 
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"black\" label =\"FF\"]\n";
        }
        kmerIterator = value.getFRList().iterator();
        while(kmerIterator.hasNext()){
            KmerBytesWritable edge = kmerIterator.next();
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"black\" label =\"FR\"]\n";
        }
        kmerIterator = value.getRFList().iterator();
        while(kmerIterator.hasNext()){
            KmerBytesWritable edge = kmerIterator.next();
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"red\" label =\"RF\"]\n";
        }
        kmerIterator = value.getRRList().iterator();
        while(kmerIterator.hasNext()){
            KmerBytesWritable edge = kmerIterator.next();
            outputEdge += outputNode + " -> " + edge.toString() + "[color = \"red\" label =\"RR\"]\n";
        }
        return outputEdge;
    }

    public static void main(String[] args) throws Exception {
        GenerateGraphViz g = new GenerateGraphViz();
        g.convertGraphCleanOutputToGraphViz("data/actual/tipadd/TipAddGraph/bin/5", "graphtest");
//        g.start("CyclePath_7");
//        g.start("SimplePath_7");
//        g.start("SinglePath_7");
//        g.start("TreePath_7");
    }
}
