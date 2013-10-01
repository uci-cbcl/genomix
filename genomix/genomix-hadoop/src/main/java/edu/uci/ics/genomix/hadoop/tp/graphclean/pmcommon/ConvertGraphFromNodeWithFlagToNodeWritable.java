/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.genomix.hadoop.tp.graphclean.pmcommon;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.uci.ics.genomix.hadoop.tp.graphclean.pmcommon.NodeWithFlagWritable.MessageFlag;
import edu.uci.ics.genomix.hadoop.tp.oldtype.position.NodeWritable;
import edu.uci.ics.genomix.hadoop.tp.oldtype.position.PositionWritable;

/*
 * Convert the graph from (PositionWritable, NodeWritableWithFlag) to (NodeWritable, NullWritable) 
 */
@SuppressWarnings("deprecation")
public class ConvertGraphFromNodeWithFlagToNodeWritable extends Configured implements Tool {

    public static class ConvertGraphMapper extends MapReduceBase implements
            Mapper<PositionWritable, NodeWithFlagWritable, NodeWritable, NullWritable> {

        /*
         * Convert the graph
         */
        @Override
        public void map(PositionWritable key, NodeWithFlagWritable value,
                OutputCollector<NodeWritable, NullWritable> output, Reporter reporter) throws IOException {
            output.collect(value.getNode(), NullWritable.get());
        }
    }

    /*
     * Convert the graph
     */
    public RunningJob run(String inputPath, String outputPath, JobConf baseConf) throws IOException {
        JobConf conf = new JobConf(baseConf);
        conf.setJarByClass(ConvertGraphFromNodeWithFlagToNodeWritable.class);
        conf.setJobName("Convert graph to NodeWritable " + inputPath);
        conf.setMapOutputKeyClass(NodeWritable.class);
        conf.setMapOutputValueClass(NullWritable.class);
        conf.setOutputKeyClass(NodeWritable.class);
        conf.setOutputValueClass(NullWritable.class);
        
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPaths(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(outputPath), true); // clean output dir
        
        conf.setMapperClass(ConvertGraphMapper.class);
//        conf.setReducerClass(PathNodeInitialReducer.class);
        conf.setNumReduceTasks(0);
        RunningJob job = JobClient.runJob(conf);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ConvertGraphFromNodeWithFlagToNodeWritable(), args);
        return res;
    }

    public static void main(String[] args) throws Exception {
        int res = new ConvertGraphFromNodeWithFlagToNodeWritable().run(args);
        System.exit(res);
    }
}
