set -x
for mincov in 3 4 5; do

    apphome=/Users/jakebiesinger/code/genomix/genomix/genomix-driver/target/genomix-driver-0.2.10-SNAPSHOT
    export PATH=$PATH:$apphome/bin:~/code/MUMmer3.23
    baseoutdir=/Users/jakebiesinger
    genome=rhodo
    K=21
    readlengths=101
    numseeds=100
    pipesteps1=REMOVE_BAD_COVERAGE,DUMP_FASTA
    pipesteps2=MERGE,DUMP_FASTA,TIP,MERGE,BUBBLE,MERGE,DUMP_FASTA
    export JAVA_OPTS="-Xmx16G"
    indir=$baseoutdir/out-$genome/build/FINAL-01-BUILD/bin
    outdir1=$baseoutdir/out-$genome/vary-coverage/mincov$mincov/out1
    outdir2=$baseoutdir/out-$genome/vary-coverage/mincov$mincov/out2
    conffile=$apphome/conf/cluster.properties


    mkdir -p $outdir1
    mkdir -p $outdir2
    rm -rf /tmp/genomix genomix build Cluster* edu.* tmp

    echo Running for min coverage $mincov
    
    # many, small frames
    sed -e "s/FRAME_SIZE=[0-9]*/FRAME_SIZE=65535/g" -e "s/FRAME_LIMIT=[0-9]*/FRAME_LIMIT=1024/g" -i .bak $conffile

    genomix -kmerLength $K -readLengths $readlengths -localInput $indir -saveIntermediateResults -threadsPerMachine 8 -runLocal -scaffold_seedScorePercentile $numseeds -localOutput $outdir1 -pipelineOrder $pipesteps1 | tee $outdir1/genomix-steps1.log



    # few, large frames
    sed -e "s/FRAME_SIZE=[0-9]*/FRAME_SIZE=524287/g" -e "s/FRAME_LIMIT=[0-9]*/FRAME_LIMIT=100/g" -i .bak $conffile
    
    genomix -kmerLength $K -readLengths $readlengths -localInput $outdir1/FINAL*/bin -saveIntermediateResults -threadsPerMachine 8 -runLocal -scaffold_seedScorePercentile $numseeds -localOutput $outdir2 -pipelineOrder $pipesteps2 | tee $outdir2/genomix.log

    for base in $outdir1 $outdir2; do
        for dir in $base/*DUMP_FASTA; do 
            rm -rf *fasta* out.* tmp*
            getCorrectnessStats.sh ~/in-$genome/genome.fasta $dir/genomix-scaffolds.fasta $dir/genomix-scaffolds.fasta | tee $dir/gage.txt
        done
    done
done
