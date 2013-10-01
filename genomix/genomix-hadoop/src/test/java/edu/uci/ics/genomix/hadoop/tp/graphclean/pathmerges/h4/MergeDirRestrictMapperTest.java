package edu.uci.ics.genomix.hadoop.tp.graphclean.pathmerges.h4;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.genomix.hadoop.tp.graphclean.graphviz.GenerateGraphViz;
import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h4.MergeDirRestrictMapper;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanDestId;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanGenericValue;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class MergeDirRestrictMapperTest {
    
    private Mapper<VKmerBytesWritable, NodeWritable, GraphCleanDestId, GraphCleanGenericValue> mapper;
    private MapDriver<VKmerBytesWritable, NodeWritable, GraphCleanDestId, GraphCleanGenericValue> driver;
    public static final String PreFix = "data/TestSet/PathMerge"; 
    public static final String[] TestDir = { PreFix + File.separator + 
        "H1mapper1"
    };
    
    public static final String SuFFix = "bin";
    
    @Before
    public void setUp() {
        mapper = new MergeDirRestrictMapper();
        driver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void h1MapperUnitTest() throws Exception {
        String srcDir = TestDir[0] + File.separator + SuFFix;
        
        File srcPath = new File(srcDir);
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.getLocal(conf);
        
        for (File f : srcPath.listFiles((FilenameFilter) (new WildcardFileFilter("part*")))) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, new Path(f.getAbsolutePath()), conf);
            VKmerBytesWritable key = new VKmerBytesWritable();
            NodeWritable value = new NodeWritable();
            while (reader.next(key, value)) {
                driver.addInput(key, value);
            }
        }
        List<Pair<GraphCleanDestId, GraphCleanGenericValue>> a = driver.run();
        for(Pair<GraphCleanDestId, GraphCleanGenericValue> iterator: a) {
            System.out.println(iterator.getFirst().toString() + "\t" + iterator.getSecond().toString());
        }
        GenerateGraphViz.convertGraphBuildingOutputToGraphViz(a, PreFix);
    }
}
