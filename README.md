Genomix
=======

What is Genomix?
-------
Genomix is a parallel genome assembly system built from the ground up with scalability in mind.
It can assemble large and high-coverage genomes from fastq files in a short time and produces assemblies similar to [Velvet](https://www.ebi.ac.uk/~zerbino/velvet/) or [Ray](http://denovoassembler.sourceforge.net/) in quality.
Genomix uses the [De Bruijin Graph](http://en.wikipedia.org/wiki/De_Bruijn_graph) to represent the assembly and cleans, prunes, and walks the graph completely in parallel.  


Under the hood, Genomix employs [Pregelix](http://hyracks.org/projects/pregelix/), a graph-based, bulk-synchronous parallel message-passing framework.  We currently handle graph compression, cleaning and scaffolding. Pregelix is an open-source implementation of Google's Pregel system, the bulk-synchronous parallel vertex-oriented programming model for large-scale graph analytics, allowing Genomix to scale to very large clusters and very large graphs. 
Pregelix uses main memory as far as it's available, and seemlessly and efficiently spills to disk.  This allows us to to produce results quickly but also allows us to scale the process to arbitrarily-sized graphs.
Genomix can run on a single machine or scale to large, cheap clusters and in our benchmarks, can run 100x faster than Hadoop-based solutions.

Usage
------
Currently Genomix code is injecting into the Pregelix codebase directly.
All genomix code is under the /genomix folder. 
/genomix-data       here is the basic data structures, like the Node, Kmer, etc.
/genomix-driver     here is the driver to connect the whole pipeline. 
/genomix-hadoop     here is the first trying of compare the result with using hadoop, (now obsolete)
/genomix-hyracks    here is the graph building step
/genomix-pregelix   here is the graph cleaning and scaffolding step.


To build Genomix:
```
git clone https://github.com/uci-cbcl/genomix.git
cd genomix
mvn package -am -pl genomix/genomix-driver -DskipTests
# Wait a few minutes...

# At this point, the complete genomix package has been packaged under:
cd genomix/genomix-driver/target/genomix-driver-0.2.10-SNAPSHOT/
```

The command line usage:
```
Usage: bin/genomix [options]

 -bridgeRemove_maxLength N              : Nodes with length <= bridgeRemoveLengt
                                          h that bridge separate paths are
                                          removed from the graph
 -bubbleMerge_maxDissimilarity N        : Maximum dissimilarity (1 - % identity)
                                          allowed between two kmers while still
                                          considering them a "bubble", (leading
                                          to their collapse into a single node)
 -bubbleMerge_maxLength N               : The maximum length an internal node
                                          may be and still be considered a
                                          bubble
 -bubbleMergewithsearch_maxLength N     : Maximum length can be searched
 -bubbleMergewithsearch_searchDirection : Maximum length can be searched
 VAL                                    :  
 -clusterWaitTime N                     : the amount of time (in ms) to wait
                                          between starting/stopping CC/NC
 -debugKmers VAL                        : Log all interactions with the given
                                          comma-separated list of kmers at the
                                          FINE log level (check conf/logging.pro
                                          perties to specify an output location)
 -extraConfFiles VAL                    : Read all the job confs from the given
                                          comma-separated list of multiple conf
                                          files
 -graphCleanMaxIterations N             : The maximum number of iterations any
                                          graph cleaning job is allowed to run
                                          for
 -hdfsInput VAL                         : HDFS directory containing input for
                                          the first pipeline step
 -hdfsOutput VAL                        : HDFS directory where the final step's
                                          output will be saved
 -hdfsWorkPath VAL                      : HDFS directory where pipeline temp
                                          output will be saved
 -kmerLength N                          : The kmer length for this graph.
 -localInput VAL                        : Local directory containing input for
                                          the first pipeline step
 -localOutput VAL                       : Local directory where the final
                                          step's output will be saved
 -logReadIds                            : Log all readIds with the selected
                                          edges at the FINE log level (check
                                          conf/logging.properties to specify an
                                          output location)
 -maxReadIDsPerEdge N                   : The maximum number of readids that
                                          are recored as spanning a single edge
 -num-lines-per-map N                   : The kmer length for this graph.
 -outerDistMeans VAL                    : Average outer distances (from A to B:
                                          A==>    <==B)  for paired-end
                                          libraries
 -outerDistStdDevs VAL                  : Standard deviations of outer distances
                                          (from A to B:  A==>    <==B)  for
                                          paired-end libraries
 -pairedEndFastqs VAL                   : Two or more local fastq files as
                                          inputs to graphbuild. Treated as
                                          paired-end reads. See also, -outerDist
                                          Mean and -outerDistStdDev
 -pathMergeRandom_probBeingRandomHead N : The probability of being selected as
                                          a random head in the random path-merge
                                          algorithm
 -pipelineOrder VAL                     : Specify the order of the graph
                                          cleaning process
 -plotSubgraph_numHops N                : The minimum vertex length that can be
                                          the head of scaffolding
 -plotSubgraph_startSeed VAL            : The minimum vertex length that can be
                                          the head of scaffolding
 -plotSubgraph_verbosity N              : Specify the level of details in
                                          output graph: 1. UNDIRECTED_GRAPH_WITH
                                          OUT_LABELS, 2. DIRECTED_GRAPH_WITH_SIM
                                          PLELABEL_AND_EDGETYPE, 3. DIRECTED_GRA
                                          PH_WITH_KMERS_AND_EDGETYPE, 4.
                                          DIRECTED_GRAPH_WITH_ALLDETAILSDefault
                                          is 1.
 -profile                               : Whether or not to do runtime profiflin
                                          g
 -randomSeed N                          : The seed used in the random path-merge
                                          or split-repeat algorithm
 -readLengths VAL                       : read lengths for each library, with
                                          paired-end libraries first
 -removeLowCoverage_maxCoverage N       : Nodes with coverage lower than this
                                          threshold will be removed from the
                                          graph
 -runAllStats                           : Whether or not to run a STATS job
                                          after each normal job
 -runLocal                              : Run a local instance using the Hadoop
                                          MiniCluster.
 -saveIntermediateResults               : whether or not to save intermediate
                                          steps to HDFS (default: true)
 -scaffold_seedLengthPercentile N       : Choose scaffolding seeds as the nodes
                                          with longest kmer length.  If this is
                                          0 < percentile < 1, this value will
                                          be interpreted as a fraction of the
                                          graph (so .01 will mean 1% of the
                                          graph will be a seed).  For fraction
                                          >= 1, it will be interpreted as the
                                          (approximate) *number* of seeds to
                                          include. Mutually exclusive with
                                          -scaffold_seedScorePercentile.
 -scaffold_seedScorePercentile N        : Choose scaffolding seeds as the
                                          highest 'seed score', currently
                                          (length * numReads).  If this is 0 <
                                          percentile < 1, this value will be
                                          interpreted as a fraction of the
                                          graph (so .01 will mean 1% of the
                                          graph will be a seed).  For fraction
                                          >= 1, it will be interpreted as the
                                          (approximate) *number* of seeds to
                                          include. Mutually exclusive with
                                          -scaffold_seedLengthPercentile.
 -scaffolding_serialRunMinLength N      : Rather than processing all the nodes
                                          in parallel, run separate scaffolding
                                          jobs serially, running with a seed of
                                          all nodes longer than this threshold
 -setCutoffCoverageByFittingMixture     : Whether or not to automatically set
                                          cutoff coverage based on fitting
                                          mixture
 -singleEndFastqs VAL                   : One or more local fastq files as
                                          inputs to graphbuild. Treated as
                                          single-ends reads.
 -stats_expectedGenomeSize N            : The expected length for this whole
                                          genome data
 -stats_minContigLength N               : the minimum contig length included in
                                          statistics calculations
 -threadsPerMachine N                   : The number of threads to use per
                                          slave machine. Default is 1.
 -tipRemove_maxLength N                 : Tips (dead ends in the graph) whose
                                          length is less than this threshold
                                          are removed from the graph
 -useExistingCluster                    : Don't start or stop a cluster (use
                                          one that's already running)

Example:
    bin/genomix -kmerLength 55 -pipelineOrder BUILD_HYRACKS,MERGE,TIP_REMOVE,MERGE,BUBBLE,MERGE -localInput /path/to/readfiledir/

```

[![Build Status](https://travis-ci.org/uci-cbcl/genomix.png?branch=genomix/fullstack_genomix)](https://travis-ci.org/uci-cbcl/genomix)

Acknowledgement
---------
YourKit is supporting Genomix open source project with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
<a href="http://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a> and
<a href="http://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>.

