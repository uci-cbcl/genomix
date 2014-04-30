set -x
for mincov in 2 3 4 5 6 7 8; do
# for mincov in 4 5 6 7 8; do
    apphome=/Users/jakebiesinger/code/genomix/genomix/genomix-driver/target/genomix-driver-0.2.12-SNAPSHOT
    export PATH=$PATH:$apphome/bin:~/code/MUMmer3.23
    baseoutdir=/Users/jakebiesinger
    genome=rhodo
    # genome=ecoli
    # genome=staph
    K=31
    readlengths=101
    numseeds=500
    delayprune=true
    # pipesteps1="REMOVE_BAD_COVERAGE,MERGE,DUMP_FASTA,TIP,MERGE,DUMP_FASTA,RAY_SCAFFOLD,MERGE,DUMP_FASTA,TIP,MERGE,DUMP_FASTA,RAY_SCAFFOLD,MERGE,DUMP_FASTA,TIP,MERGE,DUMP_FASTA,RAY_SCAFFOLD,MERGE,DUMP_FASTA"
    pipesteps1="MERGE,DUMP_FASTA,TIP,MERGE,DUMP_FASTA,RAY_SCAFFOLD,MERGE,DUMP_FASTA,TIP,MERGE,DUMP_FASTA,RAY_SCAFFOLD,MERGE,DUMP_FASTA,TIP,MERGE,DUMP_FASTA,RAY_SCAFFOLD,MERGE,DUMP_FASTA"
    pipesteps2=MERGE,DUMP_FASTA,TIP,MERGE,BUBBLE,MERGE,DUMP_FASTA
    export JAVA_OPTS="-Xmx16G"
    conffile=$apphome/conf/cluster.properties


    
    indir=$baseoutdir/out-$genome/k31-frag12-shortjump12-se/BL2-1k

# genomix -kmerLength $K -readLengths $readlengths -singleEndFastqs /Users/jakebiesinger/in-rhodo/frag_1.fastq,/Users/jakebiesinger/in-rhodo/frag_2.fastq,/Users/jakebiesinger/in-rhodo/shortjump_1.fastq,/Users/jakebiesinger/in-rhodo/shortjump_2.fastq -saveIntermediateResults -threadsPerMachine 8 -runLocal -scaffold_seedScorePercentile $numseeds -localOutput $indir -pipelineOrder BUILD,REMOVE_BAD_COVERAGE -removeBadCoverage_maxCoverage 1000 -removeBadCoverage_minCoverage 2 -scaffold_removeOtherIncoming true -scaffold_removeOtherOutgoing true -scaffold_delayPrune true  2>&1 | tee $indir-genomix-steps1.log
# exit 0

    # indir=/Users/jakebiesinger/out-rhodo/k31-frag12-shortjump12-se/BL2-1k-M3xTMSM-repeatafter2-delayprune-true
    for addtlcov in 0 1 2 3 4 5; do 
    # for addtlcov in 1 2 3 4 5; do 
        let finalcov="$mincov + $addtlcov"
        outdir=$baseoutdir/out-$genome/k31-frag12-shortjump12-se/BL"$mincov"-1k-M3xTMSM-repeatafter"$finalcov"-delayprune-"$delayprune"
        mkdir -p $outdir

        rm -rf /tmp/genomix build Cluster* edu.* tmp

        echo Running for min coverage $mincov additional step $addtlcov
        
        # many, small frames
        # sed -e "s/FRAME_SIZE=[0-9]*/FRAME_SIZE=65535/g" -e "s/FRAME_LIMIT=[0-9]*/FRAME_LIMIT=1024/g" -i .bak $conffile
        
        sed -e "s/FRAME_SIZE=[0-9]*/FRAME_SIZE=524287/g" -e "s/FRAME_LIMIT=[0-9]*/FRAME_LIMIT=100/g" -i .bak $conffile
        # genomix -kmerLength $K -readLengths $readlengths -localInput $indir/FINAL*/bin -saveIntermediateResults -threadsPerMachine 1 -runLocal -scaffold_seedScorePercentile $numseeds -localOutput $outdir -pipelineOrder "$pipesteps1" -removeBadCoverage_maxCoverage 1000 -removeBadCoverage_minCoverage $finalcov -scaffold_removeOtherIncoming true -scaffold_removeOtherOutgoing true -scaffold_delayPrune false | tee $outdir-genomix-steps1.log
        genomix -kmerLength $K -readLengths $readlengths -localInput $indir/FINAL*/bin -saveIntermediateResults -threadsPerMachine 8 -runLocal -scaffold_seedScorePercentile $numseeds -localOutput $outdir -pipelineOrder "$pipesteps1" -removeBadCoverage_maxCoverage 1000 -removeBadCoverage_minCoverage $finalcov -scaffold_removeOtherIncoming true -scaffold_removeOtherOutgoing true -scaffold_delayPrune $delayprune    2>&1 | tee $outdir-genomix-steps1.log
# -graphCleanMaxIterations 50
        indir=$outdir

        # few, large frames
        # sed -e "s/FRAME_SIZE=[0-9]*/FRAME_SIZE=524287/g" -e "s/FRAME_LIMIT=[0-9]*/FRAME_LIMIT=100/g" -i .bak $conffile
        
        #genomix -kmerLength $K -readLengths $readlengths -localInput $outdir1/FINAL*/bin -saveIntermediateResults -threadsPerMachine 8 -runLocal -scaffold_seedScorePercentile $numseeds -localOutput $outdir2 -pipelineOrder $pipesteps2 | tee $outdir2/genomix.log

        # for base in $outdir1 $outdir2; do
        # for base in $outdir3; do
        for base in $outdir; do
            for dir in $base/*DUMP_FASTA; do 
                rm -rf *fasta* out.* tmp* *genomix-scaffolds*
                getCorrectnessStats.sh ~/in-$genome/genome.fasta $dir/genomix-scaffolds.fasta $dir/genomix-scaffolds.fasta | tee $dir/gage.txt
            done
        done
    done
done
