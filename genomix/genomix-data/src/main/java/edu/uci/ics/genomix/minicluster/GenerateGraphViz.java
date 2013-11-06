package edu.uci.ics.genomix.minicluster;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

//TODO by Jianfeng: move this to script
public class GenerateGraphViz {

    private static long count = 0;
    private static HashMap<String, Long> map = new HashMap<String, Long>();

    public enum GRAPH_TYPE {

        UNDIRECTED_GRAPH_WITHOUT_LABELS((int) 1),
        DIRECTED_GRAPH_WITH_SIMPLELABEL_AND_EDGETYPE((int) 2),
        DIRECTED_GRAPH_WITH_KMERS_AND_EDGETYPE((int) 3),
        DIRECTED_GRAPH_WITH_ALLDETAILS((int) 4);
        
        private final int val;
        
        private GRAPH_TYPE(int val) {
            this.val = val;
        }

        public final int get() {
            return val;
        }
        
        public static GRAPH_TYPE getFromInt(int n){
            switch(n){
                case 1:
                    return UNDIRECTED_GRAPH_WITHOUT_LABELS;
                case 2:
                    return DIRECTED_GRAPH_WITH_SIMPLELABEL_AND_EDGETYPE;
                case 3:
                    return DIRECTED_GRAPH_WITH_KMERS_AND_EDGETYPE;
                case 4:
                    return DIRECTED_GRAPH_WITH_ALLDETAILS;
                default:
                    throw new IllegalArgumentException("Invalid integer for GRAPH_TYPE given: " + n);
            }
        }
    }

    public static void convertBinToGraphViz(String srcDir, String destDir, GRAPH_TYPE graphType) throws Exception {
        convertBinToGraphViz(new JobConf(), srcDir, destDir, graphType);
    }
    
    /**
     * Construct a DOT graph in memory, convert it
     * to image and store the image in the file system.
     */
        public static void convertBinToGraphViz(JobConf conf, String srcDir, String destDir, GRAPH_TYPE graphType) throws Exception {
        GraphViz gv = new GraphViz();  
        gv.addln(gv.start_graph());

        FileSystem dfs = FileSystem.get(conf);
        File srcPath = new File(srcDir);

        String outputNode = "";
        String outputEdge = "";
        for (File f : srcPath.listFiles((FilenameFilter) (new WildcardFileFilter("part*")))) {
            SequenceFile.Reader reader = new SequenceFile.Reader(dfs, new Path(f.getAbsolutePath()), conf);
            VKmer key = (VKmer) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Node value = (Node) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            gv.addln("rankdir=LR\n");
            while (reader.next(key, value)) {
                outputNode = "";
                outputEdge = "";
                if (key == null) {
                    break;
                }
                switch (graphType) {
                    case UNDIRECTED_GRAPH_WITHOUT_LABELS:
                    case DIRECTED_GRAPH_WITH_SIMPLELABEL_AND_EDGETYPE:
                        if (map.containsKey(key.toString()))
                            outputNode += map.get(key.toString());
                        else {
                            count++;
                            map.put(key.toString(), count);
                            outputNode += count;
                        }
                        break;
                    case DIRECTED_GRAPH_WITH_KMERS_AND_EDGETYPE:
                    case DIRECTED_GRAPH_WITH_ALLDETAILS:
                        outputNode += key.toString();
                        break;
                    default:
                        throw new IllegalStateException("Invalid input Graph Type!!!");
                }
                /** convert edge to graph **/
                outputEdge = convertEdgeToGraph(outputNode, value, graphType);
                gv.addln(outputEdge);
                if (graphType == GRAPH_TYPE.DIRECTED_GRAPH_WITH_ALLDETAILS) {
                    /** add readIdSet **/
                    String fillColor = "";
                    if (value.isUnflippedOrFlippedReadIds())
                        fillColor = "fillcolor=\"grey\", style=\"filled\",";
                    outputNode += " [shape=record, " + fillColor + " label = \"<f0> " + key.toString() + "|<f1> "
                            + "5':" + value.getUnflippedReadIds().toReadIdString() + "|<f2> " + "~5':"
                            + value.getFlippedReadIds().toReadIdString() + "|<f3> " + value.getAverageCoverage() + "|<f4> "
                            + value.getInternalKmer() + "\"]\n";
                }
                gv.addln(outputNode);
            }
            reader.close();
        }

        gv.addln(gv.end_graph());

        String type = "svg";
        //        File folder = new File(destDir);
        //        folder.mkdirs();
        File out = new File(destDir + "." + type); // Linux
        gv.writeGraphToFile(gv.getGraph(gv.getDotSource(), type), out);
    }

    public static String convertEdgeToGraph(String outputNode, Node value, GRAPH_TYPE graphType) {
        String outputEdge = ""; 
        for (EDGETYPE et : EDGETYPE.values()) {
            for (Entry<VKmer, ReadIdSet> e : value.getEdgeMap(et).entrySet()) {
                String destNode = "";
                switch (graphType) {
                    case UNDIRECTED_GRAPH_WITHOUT_LABELS:
                        if (map.containsKey(e.getKey().toString()))
                            destNode += map.get(e.getKey().toString());
                        else {
                            count++;
                            map.put(e.getKey().toString(), count);
                            destNode += count;
                        }
                        outputEdge += outputNode + " -> " + destNode + "[dir=none]\n";
                        break;
                    case DIRECTED_GRAPH_WITH_SIMPLELABEL_AND_EDGETYPE:
                        if (map.containsKey(e.getKey().toString()))
                            destNode += map.get(e.getKey().toString());
                        else {
                            count++;
                            map.put(e.getKey().toString(), count);
                            destNode += count;
                        }
                        outputEdge += outputNode + " -> " + destNode + "[color = \"" + getColor(et) + "\" label =\""
                                + et + "\"]\n";
                        break;
                    case DIRECTED_GRAPH_WITH_KMERS_AND_EDGETYPE:
                        outputEdge += outputNode + " -> " + e.getKey().toString() + "[color = \"" + getColor(et)
                                + "\" label =\"" + et + "\"]\n";
                        break;
                    case DIRECTED_GRAPH_WITH_ALLDETAILS:
                        outputEdge += outputNode + " -> " + e.getKey().toString() + "[color = \"" + getColor(et)
                                + "\" label =\"" + et + ": " + e.getValue() + "\"]\n";
                        break;
                    default:
                        throw new IllegalStateException("Invalid input Graph Type!!!");
                }
            }
        }
        //TODO should output actualKmer instead of kmer
        if (outputEdge == "")
            outputEdge += outputNode;
        return outputEdge;
    }

    public static String getColor(EDGETYPE et) {
        switch (et) {
            case FF:
                return "black";
            case FR:
                return "blue";
            case RF:
                return "green";
            case RR:
                return "red";
            default:
                throw new IllegalStateException("Invalid input Edge Type!!!");
        }
    }

}
