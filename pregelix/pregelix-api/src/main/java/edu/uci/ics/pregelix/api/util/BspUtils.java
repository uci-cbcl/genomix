/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.pregelix.api.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.NormalizedKeyComputer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.graph.VertexPartitioner;
import edu.uci.ics.pregelix.api.io.VertexInputFormat;
import edu.uci.ics.pregelix.api.io.VertexOutputFormat;
import edu.uci.ics.pregelix.api.io.WritableSizable;
import edu.uci.ics.pregelix.api.job.ICheckpointHook;
import edu.uci.ics.pregelix.api.job.IIterationCompleteReporterHook;
import edu.uci.ics.pregelix.api.job.PregelixJob;

/**
 * Help to use the configuration to get the appropriate classes or instantiate
 * them.
 */
public class BspUtils {
    
    public static final String TMP_DIR = "/tmp/";
    private static final String COUNTERS_VALUE_ON_ITERATION = ".counters.valueOnIter.";
    private static final String COUNTERS_LAST_ITERATION_COMPLETED = ".counters.lastIterCompleted";

    /**
     * Get the user's subclassed {@link VertexInputFormat}.
     * 
     * @param conf
     *            Configuration to check
     * @return User's vertex input format class
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable> Class<? extends VertexInputFormat<I, V, E, M>> getVertexInputFormatClass(
            Configuration conf) {
        return (Class<? extends VertexInputFormat<I, V, E, M>>) conf.getClass(PregelixJob.VERTEX_INPUT_FORMAT_CLASS,
                null, VertexInputFormat.class);
    }

    /**
     * Create a user vertex input format class
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user vertex input format class
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable> VertexInputFormat<I, V, E, M> createVertexInputFormat(
            Configuration conf) {
        Class<? extends VertexInputFormat<I, V, E, M>> vertexInputFormatClass = getVertexInputFormatClass(conf);
        VertexInputFormat<I, V, E, M> inputFormat = ReflectionUtils.newInstance(vertexInputFormatClass, conf);
        return inputFormat;
    }

    /**
     * Get the user's subclassed {@link VertexOutputFormat}.
     * 
     * @param conf
     *            Configuration to check
     * @return User's vertex output format class
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <I extends WritableComparable, V extends Writable, E extends Writable> Class<? extends VertexOutputFormat<I, V, E>> getVertexOutputFormatClass(
            Configuration conf) {
        return (Class<? extends VertexOutputFormat<I, V, E>>) conf.getClass(PregelixJob.VERTEX_OUTPUT_FORMAT_CLASS,
                null, VertexOutputFormat.class);
    }

    /**
     * Create a user vertex output format class
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user vertex output format class
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable, E extends Writable> VertexOutputFormat<I, V, E> createVertexOutputFormat(
            Configuration conf) {
        Class<? extends VertexOutputFormat<I, V, E>> vertexOutputFormatClass = getVertexOutputFormatClass(conf);
        return ReflectionUtils.newInstance(vertexOutputFormatClass, conf);
    }

    /**
     * Get the user's subclassed {@link MessageCombiner}.
     * 
     * @param conf
     *            Configuration to check
     * @return User's vertex combiner class
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <I extends WritableComparable, M extends WritableSizable, P extends Writable> Class<? extends MessageCombiner<I, M, P>> getMessageCombinerClass(
            Configuration conf) {
        return (Class<? extends MessageCombiner<I, M, P>>) conf.getClass(PregelixJob.Message_COMBINER_CLASS,
                DefaultMessageCombiner.class, MessageCombiner.class);
    }

    /**
     * Get the user's subclassed {@link GlobalAggregator}.
     * 
     * @param conf
     *            Configuration to check
     * @return User's vertex combiner class
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable, P extends Writable, F extends Writable> List<Class<? extends GlobalAggregator<I, V, E, M, P, F>>> getGlobalAggregatorClasses(
            Configuration conf) {
        String aggStrs = conf.get(PregelixJob.GLOBAL_AGGREGATOR_CLASS);
        String[] classnames;
        if (aggStrs == null) {
            classnames = new String[0];
        } else {
            classnames = aggStrs.split(PregelixJob.COMMA_STR);
        }
        try {
            List<Class<? extends GlobalAggregator<I, V, E, M, P, F>>> classes = new ArrayList<Class<? extends GlobalAggregator<I, V, E, M, P, F>>>();
            for (String defaultClass : PregelixJob.DEFAULT_GLOBAL_AGGREGATOR_CLASSES) {
                classes.add((Class<? extends GlobalAggregator<I, V, E, M, P, F>>) conf.getClassByName(defaultClass));
            }
            for (int i = 0; i < classnames.length; i++) {
                classes.add((Class<? extends GlobalAggregator<I, V, E, M, P, F>>) conf.getClassByName(classnames[i]));
            }
            return classes;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getJobId(Configuration conf) {
        return conf.get(PregelixJob.JOB_ID);
    }

    /**
     * Create a user vertex combiner class
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user vertex combiner class
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, M extends WritableSizable, P extends Writable> MessageCombiner<I, M, P> createMessageCombiner(
            Configuration conf) {
        Class<? extends MessageCombiner<I, M, P>> vertexCombinerClass = getMessageCombinerClass(conf);
        return ReflectionUtils.newInstance(vertexCombinerClass, conf);
    }

    /**
     * Create a user-defined normalized key computer class
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user-defined normalized key computer
     */
    public static NormalizedKeyComputer createNormalizedKeyComputer(Configuration conf) {
        Class<? extends NormalizedKeyComputer> nmkClass = getNormalizedKeyComputerClass(conf);
        return ReflectionUtils.newInstance(nmkClass, conf);
    }

    /**
     * Create a global aggregator object
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user vertex combiner class
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable, P extends Writable, F extends Writable> List<GlobalAggregator> createGlobalAggregators(
            Configuration conf) {
        List<Class<? extends GlobalAggregator<I, V, E, M, P, F>>> globalAggregatorClasses = getGlobalAggregatorClasses(conf);
        List<GlobalAggregator> aggs = new ArrayList<GlobalAggregator>();
        for (Class<? extends GlobalAggregator<I, V, E, M, P, F>> globalAggClass : globalAggregatorClasses) {
            aggs.add(ReflectionUtils.newInstance(globalAggClass, conf));
        }
        return aggs;
    }

    /**
     * Get global aggregator class names
     * 
     * @param conf
     *            Configuration to check
     * @return An array of Global aggregator names
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable, P extends Writable, F extends Writable> String[] getGlobalAggregatorClassNames(
            Configuration conf) {
        List<Class<? extends GlobalAggregator<I, V, E, M, P, F>>> globalAggregatorClasses = getGlobalAggregatorClasses(conf);
        String[] aggClassNames = new String[globalAggregatorClasses.size()];
        int i = 0;
        for (Class<? extends GlobalAggregator<I, V, E, M, P, F>> globalAggClass : globalAggregatorClasses) {
            aggClassNames[i++] = globalAggClass.getName();
        }
        return aggClassNames;
    }

    /**
     * Get global aggregator class names
     * 
     * @param conf
     *            Configuration to check
     * @return An array of Global aggregator names
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable, P extends Writable, F extends Writable> String[] getPartialAggregateValueClassNames(
            Configuration conf) {
        String[] gloablAggClassNames = getGlobalAggregatorClassNames(conf);
        String[] partialAggValueClassNames = new String[gloablAggClassNames.length];
        int i = 0;
        for (String globalAggClassName : gloablAggClassNames) {
            partialAggValueClassNames[i++] = getPartialAggregateValueClass(conf, globalAggClassName).getName();
        }
        return partialAggValueClassNames;
    }

    /**
     * Get the user's subclassed Vertex.
     * 
     * @param conf
     *            Configuration to check
     * @return User's vertex class
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable> Class<? extends Vertex<I, V, E, M>> getVertexClass(
            Configuration conf) {
        return (Class<? extends Vertex<I, V, E, M>>) conf.getClass(PregelixJob.VERTEX_CLASS, null, Vertex.class);
    }

    /**
     * Create a user vertex
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user vertex
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable> Vertex<I, V, E, M> createVertex(
            Configuration conf) {
        Class<? extends Vertex<I, V, E, M>> vertexClass = getVertexClass(conf);
        Vertex<I, V, E, M> vertex = ReflectionUtils.newInstance(vertexClass, conf);
        return vertex;
    }

    /**
     * Get the user's subclassed vertex index class.
     * 
     * @param conf
     *            Configuration to check
     * @return User's vertex index class
     */
    @SuppressWarnings("unchecked")
    public static <I extends Writable> Class<I> getVertexIndexClass(Configuration conf) {
        return (Class<I>) conf.getClass(PregelixJob.VERTEX_INDEX_CLASS, WritableComparable.class);
    }

    /**
     * Create a user vertex index
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user vertex index
     */
    @SuppressWarnings("rawtypes")
    public static <I extends WritableComparable> I createVertexIndex(Configuration conf) {
        Class<I> vertexClass = getVertexIndexClass(conf);
        try {
            return vertexClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("createVertexIndex: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("createVertexIndex: Illegally accessed", e);
        }
    }

    /**
     * Get the user's subclassed vertex value class.
     * 
     * @param conf
     *            Configuration to check
     * @return User's vertex value class
     */
    @SuppressWarnings("unchecked")
    public static <V extends Writable> Class<V> getVertexValueClass(Configuration conf) {
        return (Class<V>) conf.getClass(PregelixJob.VERTEX_VALUE_CLASS, Writable.class);
    }

    /**
     * Create a user vertex value
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user vertex value
     */
    public static <V extends Writable> V createVertexValue(Configuration conf) {
        Class<V> vertexValueClass = getVertexValueClass(conf);
        try {
            return vertexValueClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("createVertexValue: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("createVertexValue: Illegally accessed", e);
        }
    }

    /**
     * Get the user's subclassed edge value class.
     * 
     * @param conf
     *            Configuration to check
     * @return User's vertex edge value class
     */
    @SuppressWarnings("unchecked")
    public static <E extends Writable> Class<E> getEdgeValueClass(Configuration conf) {
        return (Class<E>) conf.getClass(PregelixJob.EDGE_VALUE_CLASS, Writable.class);
    }

    /**
     * Create a user edge value
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user edge value
     */
    public static <E extends Writable> E createEdgeValue(Configuration conf) {
        Class<E> edgeValueClass = getEdgeValueClass(conf);
        try {
            return edgeValueClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("createEdgeValue: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("createEdgeValue: Illegally accessed", e);
        }
    }

    /**
     * Get the user's subclassed vertex message value class.
     * 
     * @param conf
     *            Configuration to check
     * @return User's vertex message value class
     */
    @SuppressWarnings("unchecked")
    public static <M extends WritableSizable> Class<M> getMessageValueClass(Configuration conf) {
        return (Class<M>) conf.getClass(PregelixJob.MESSAGE_VALUE_CLASS, Writable.class);
    }

    /**
     * Get the user's subclassed global aggregator's partial aggregate value class.
     * 
     * @param conf
     *            Configuration to check
     * @return User's global aggregate value class
     */
    @SuppressWarnings("unchecked")
    public static <M extends Writable> Class<M> getPartialAggregateValueClass(Configuration conf, String aggClassName) {
        return (Class<M>) conf.getClass(PregelixJob.PARTIAL_AGGREGATE_VALUE_CLASS + "$" + aggClassName, Writable.class);
    }

    /**
     * Get the user's subclassed combiner's partial combine value class.
     * 
     * @param conf
     *            Configuration to check
     * @return User's global aggregate value class
     */
    @SuppressWarnings("unchecked")
    public static <M extends Writable> Class<M> getPartialCombineValueClass(Configuration conf) {
        return (Class<M>) conf.getClass(PregelixJob.PARTIAL_COMBINE_VALUE_CLASS, Writable.class);
    }

    /**
     * Get the user's subclassed normalized key computer class.
     * 
     * @param conf
     *            Configuration to check
     * @return User's normalized key computer class
     */
    @SuppressWarnings("unchecked")
    public static Class<? extends NormalizedKeyComputer> getNormalizedKeyComputerClass(Configuration conf) {
        return (Class<? extends NormalizedKeyComputer>) conf.getClass(PregelixJob.NMK_COMPUTER_CLASS,
                NormalizedKeyComputer.class);
    }

    /**
     * Get the user's subclassed normalized key computer class.
     * 
     * @param conf
     *            Configuration to check
     * @return User's global aggregate value class
     */
    @SuppressWarnings("unchecked")
    public static <M extends Writable> Class<M> getFinalAggregateValueClass(Configuration conf, String aggClassName) {
        return (Class<M>) conf.getClass(PregelixJob.FINAL_AGGREGATE_VALUE_CLASS + "$" + aggClassName, Writable.class);
    }

    /**
     * Create a user vertex message value
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user vertex message value
     */
    public static <M extends WritableSizable> M createMessageValue(Configuration conf) {
        Class<M> messageValueClass = getMessageValueClass(conf);
        try {
            return messageValueClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("createMessageValue: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("createMessageValue: Illegally accessed", e);
        }
    }

    /**
     * Create a user partial aggregate value
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user aggregate value
     */
    public static <M extends Writable> M createPartialAggregateValue(Configuration conf, String aggClassName) {
        Class<M> aggregateValueClass = getPartialAggregateValueClass(conf, aggClassName);
        try {
            return aggregateValueClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("createPartialAggregateValue: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("createPartialAggregateValue: Illegally accessed", e);
        }
    }

    /**
     * Create the list of user partial aggregate values
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user partial aggregate values
     */
    public static <M extends Writable> List<M> createPartialAggregateValues(Configuration conf) {
        String[] aggClassNames = BspUtils.getGlobalAggregatorClassNames(conf);
        List<M> aggValueList = new ArrayList<M>();
        for (String aggClassName : aggClassNames) {
            Class<M> aggregateValueClass = getPartialAggregateValueClass(conf, aggClassName);
            try {
                aggValueList.add(aggregateValueClass.newInstance());
            } catch (InstantiationException e) {
                throw new IllegalArgumentException("createAggregateValue: Failed to instantiate", e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("createAggregateValue: Illegally accessed", e);
            }
        }
        return aggValueList;
    }

    /**
     * Create a user partial combine value
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user aggregate value
     */
    @SuppressWarnings("rawtypes")
    public static <M extends Writable> M createPartialCombineValue(Configuration conf) {
        Class<M> aggregateValueClass = getPartialCombineValueClass(conf);
        try {
            M instance = aggregateValueClass.newInstance();
            if (instance instanceof MsgList) {
                // set conf for msg list, if the value type is msglist
                ((MsgList) instance).setConf(conf);
            }
            return instance;
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("createPartialCombineValue: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("createPartialCombineValue: Illegally accessed", e);
        }
    }

    /**
     * Create a user aggregate value
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user aggregate value
     */
    public static <M extends Writable> M createFinalAggregateValue(Configuration conf, String aggClassName) {
        Class<M> aggregateValueClass = getFinalAggregateValueClass(conf, aggClassName);
        try {
            return aggregateValueClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("createAggregateValue: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("createAggregateValue: Illegally accessed", e);
        }
    }

    /**
     * Create the list of user aggregate values
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user aggregate value
     */
    public static <M extends Writable> List<M> createFinalAggregateValues(Configuration conf) {
        String[] aggClassNames = BspUtils.getGlobalAggregatorClassNames(conf);
        List<M> aggValueList = new ArrayList<M>();
        for (String aggClassName : aggClassNames) {
            Class<M> aggregateValueClass = getFinalAggregateValueClass(conf, aggClassName);
            try {
                aggValueList.add(aggregateValueClass.newInstance());
            } catch (InstantiationException e) {
                throw new IllegalArgumentException("createAggregateValue: Failed to instantiate", e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("createAggregateValue: Illegally accessed", e);
            }
        }
        return aggValueList;
    }

    /**
     * Create a user aggregate value
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user aggregate value
     */
    @SuppressWarnings("rawtypes")
    public static VertexPartitioner createVertexPartitioner(Configuration conf) {
        Class<? extends VertexPartitioner> vertexPartitionerClass = getVertexPartitionerClass(conf);
        try {
            return vertexPartitionerClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("createVertexPartitioner: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("createVertexPartitioner: Illegally accessed", e);
        }
    }

    /**
     * Create a checkpoint hook
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user aggregate value
     */
    public static ICheckpointHook createCheckpointHook(Configuration conf) {
        Class<? extends ICheckpointHook> ckpClass = getCheckpointHookClass(conf);
        try {
            return ckpClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("createVertexPartitioner: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("createVertexPartitioner: Illegally accessed", e);
        }
    }

    /**
     * Create a hook that indicates an iteration is complete
     * 
     * @param conf
     *            Configuration to check
     * @return Instantiated user aggregate value
     */
    public static IIterationCompleteReporterHook createIterationCompleteHook(Configuration conf) {
        Class<? extends IIterationCompleteReporterHook> itCompleteClass = getIterationCompleteReporterHookClass(conf);
        try {
            return itCompleteClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("createVertexPartitioner: Failed to instantiate", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("createVertexPartitioner: Illegally accessed", e);
        }
    }

    /**
     * Get the user's subclassed vertex partitioner class.
     * 
     * @param conf
     *            Configuration to check
     * @return The user defined vertex partitioner class
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <V extends VertexPartitioner> Class<V> getVertexPartitionerClass(Configuration conf) {
        return (Class<V>) conf.getClass(PregelixJob.PARTITIONER_CLASS, null, VertexPartitioner.class);
    }

    /**
     * Get the user's subclassed checkpoint hook class.
     * 
     * @param conf
     *            Configuration to check
     * @return The user defined vertex checkpoint hook class
     */
    @SuppressWarnings("unchecked")
    public static <V extends ICheckpointHook> Class<V> getCheckpointHookClass(Configuration conf) {
        return (Class<V>) conf.getClass(PregelixJob.CKP_CLASS, DefaultCheckpointHook.class, ICheckpointHook.class);
    }

    /**
     * Get the user's subclassed iteration complete reporter hook class.
     * 
     * @param conf
     *            Configuration to check
     * @return The user defined vertex iteration complete reporter class
     */
    @SuppressWarnings("unchecked")
    public static <V extends IIterationCompleteReporterHook> Class<V> getIterationCompleteReporterHookClass(
            Configuration conf) {
        return (Class<V>) conf.getClass(PregelixJob.ITERATION_COMPLETE_CLASS,
                DefaultIterationCompleteReporterHook.class, IIterationCompleteReporterHook.class);
    }

    /**
     * Get the job configuration parameter whether the vertex states will increase dynamically
     * 
     * @param conf
     *            the job configuration
     * @return the boolean setting of the parameter, by default it is false
     */
    public static boolean getDynamicVertexValueSize(Configuration conf) {
        return conf.getBoolean(PregelixJob.INCREASE_STATE_LENGTH, true);
    }

    /**
     * Get the specified frame size
     * 
     * @param conf
     *            the job configuration
     * @return the specified frame size; -1 if it is not set by users
     */
    public static int getFrameSize(Configuration conf) {
        return conf.getInt(PregelixJob.FRAME_SIZE, -1);
    }

    /**
     * Should the job use LSM or B-tree to store vertices
     * 
     * @param conf
     * @return
     */
    public static boolean useLSM(Configuration conf) {
        return conf.getBoolean(PregelixJob.UPDATE_INTENSIVE, false);
    }

    /***
     * Get the spilling dir name for global aggregates
     * 
     * @param conf
     * @param superStep
     * @return the spilling dir name
     */
    public static String getGlobalAggregateSpillingDirName(Configuration conf, long superStep) {
        return "/tmp/pregelix/agg/" + conf.get(PregelixJob.JOB_ID) + "/" + superStep;
    }

    /**
     * Get the path for vertex checkpointing
     * 
     * @param conf
     * @param lastSuperStep
     * @return the path for vertex checkpointing
     */
    public static String getVertexCheckpointPath(Configuration conf, long lastSuperStep) {
        return "/tmp/ckpoint/" + BspUtils.getJobId(conf) + "/vertex/" + lastSuperStep;
    }

    /**
     * Get the path for message checkpointing
     * 
     * @param conf
     * @param lastSuperStep
     * @return the path for message checkpointing
     */
    public static String getMessageCheckpointPath(Configuration conf, long lastSuperStep) {
        String path = "/tmp/ckpoint/" + BspUtils.getJobId(conf) + "/message/" + lastSuperStep;
        return path;
    }

    /**
     * Get the path for message checkpointing
     * 
     * @param conf
     * @param lastSuperStep
     * @return the path for message checkpointing
     */
    public static String getSecondaryIndexCheckpointPath(Configuration conf, long lastSuperStep) {
        return "/tmp/ckpoint/" + BspUtils.getJobId(conf) + "/secondaryindex/" + lastSuperStep;
    }

    /***
     * Get the recovery count
     * 
     * @return recovery count
     */
    public static int getRecoveryCount(Configuration conf) {
        return conf.getInt(PregelixJob.RECOVERY_COUNT, 0);
    }
    
    /***
     * Get enable dynamic optimization
     * 
     * @param conf Configuration
     * @return true if enabled; otherwise false
     */
    public static boolean getEnableDynamicOptimization(Configuration conf){
        return conf.getBoolean(PregelixJob.DYNAMIC_OPTIMIZATION, true);
    }

    /***
     * Get the user-set checkpoint interval
     * 
     * @param conf
     * @return the checkpoint interval
     */
    public static int getCheckpointingInterval(Configuration conf) {
        return conf.getInt(PregelixJob.CKP_INTERVAL, -1);
    }

    public static Writable readGlobalAggregateValue(Configuration conf, String jobId, String aggClassName)
            throws HyracksDataException {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = TMP_DIR + jobId + "agg";
            Path path = new Path(pathStr);
            FSDataInputStream input = dfs.open(path);
            int numOfAggs = createFinalAggregateValues(conf).size();
            for (int i = 0; i < numOfAggs; i++) {
                String aggName = input.readUTF();
                Writable agg = createFinalAggregateValue(conf, aggName);
                if (aggName.equals(aggClassName)) {
                    agg.readFields(input);
                    input.close();
                    return agg;
                } else {
                    agg.readFields(input);
                }
            }
            throw new IllegalStateException("Cannot find the aggregate value for " + aggClassName);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static HashMap<String, Writable> readAllGlobalAggregateValues(Configuration conf, String jobId)
            throws HyracksDataException {
        String pathStr = TMP_DIR + jobId + "agg";
        Path path = new Path(pathStr);
        List<Writable> aggValues = createFinalAggregateValues(conf);
        HashMap<String, Writable> finalAggs = new HashMap<>();
        try {
            FileSystem dfs = FileSystem.get(conf);
            FSDataInputStream input = dfs.open(path);
            for (int i = 0; i < aggValues.size(); i++) {
                String aggName = input.readUTF();
                aggValues.get(i).readFields(input);
                finalAggs.put(aggName, aggValues.get(i));
            }
            input.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return finalAggs;
    }

    public static Counters getCounters(PregelixJob job) throws HyracksDataException {
        Configuration conf = job.getConfiguration();
        String jobId = getJobId(conf);
        int lastIter = BspUtils.readCountersLastIteration(conf, jobId);
        return BspUtils.readCounters(lastIter, conf, jobId);
    }

    static Counters readCounters(int superstep, Configuration conf, String jobId) throws HyracksDataException {
        String pathStr = TMP_DIR + jobId + BspUtils.COUNTERS_VALUE_ON_ITERATION + superstep;
        Path path = new Path(pathStr);
        Counters savedCounters = new Counters();
        try {
            FileSystem dfs = FileSystem.get(conf);
            FSDataInputStream input = dfs.open(path);
            savedCounters.readFields(input);
            input.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return savedCounters;
    }

    static void writeCounters(Counters toWrite, int superstep, Configuration conf, String jobId)
            throws HyracksDataException {
        String pathStr = TMP_DIR + jobId + BspUtils.COUNTERS_VALUE_ON_ITERATION + superstep;
        Path path = new Path(pathStr);
        try {
            FileSystem dfs = FileSystem.get(conf);
            FSDataOutputStream output = dfs.create(path, true);
            toWrite.write(output);
            output.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    static int readCountersLastIteration(Configuration conf, String jobId) throws HyracksDataException {
        String pathStr = TMP_DIR + jobId + BspUtils.COUNTERS_LAST_ITERATION_COMPLETED;
        Path path = new Path(pathStr);
        IntWritable lastIter = new IntWritable();
        try {
            FileSystem dfs = FileSystem.get(conf);
            FSDataInputStream input = dfs.open(path);
            lastIter.readFields(input);
            input.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return lastIter.get();
    }

    static void writeCountersLastIteration(int superstep, Configuration conf, String jobId) throws HyracksDataException {
        String pathStr = TMP_DIR + jobId + BspUtils.COUNTERS_LAST_ITERATION_COMPLETED;
        Path path = new Path(pathStr);
        try {
            FileSystem dfs = FileSystem.get(conf);
            FSDataOutputStream output = dfs.create(path, true);
            new IntWritable(superstep).write(output);
            output.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
