package edu.uci.ics.genomix.pregelix.client;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.IntOptionHandler;
import org.kohsuke.args4j.spi.Setter;

import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P4ForPathMergeVertex;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.base.IDriver.Plan;
import edu.uci.ics.pregelix.core.driver.Driver;

public class Genomix {

    public static void run(String[] args, PregelixJob job) throws Exception {
        Options options = prepareJob(args, job);
        Driver driver = new Driver(Genomix.class);
        driver.runJob(job, options.planChoice, options.ipAddress, options.port, Boolean.parseBoolean(options.profiling));
    }

    private static Options prepareJob(String[] args, PregelixJob job) throws CmdLineException, IOException {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        String[] inputs = options.inputPaths.split(";");
        FileInputFormat.setInputPaths(job, inputs[0]);
        for (int i = 1; i < inputs.length; i++)
            FileInputFormat.addInputPaths(job, inputs[0]);
        FileOutputFormat.setOutputPath(job, new Path(options.outputPath));
        job.getConfiguration().setInt(BasicGraphCleanVertex.KMER_SIZE, options.sizeKmer);
        if (options.numIteration > 0) {
            job.getConfiguration().setInt(BasicGraphCleanVertex.ITERATIONS, options.numIteration);
        }

        //        if (options.pseudoRate > 0 && options.pseudoRate <= 1)
        //           job.getConfiguration().setFloat(P3ForPathMergeVertex.PSEUDORATE, options.pseudoRate);
        //        if (options.maxRound > 0)
        //            job.getConfiguration().setInt(P3ForPathMergeVertex.MAXROUND, options.maxRound);
        return options;

    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        validateArguments();
        run();
    }
}
