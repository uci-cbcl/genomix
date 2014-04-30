set -x
apphome=/Users/jakebiesinger/code/genomix/genomix/genomix-driver/target/genomix-driver-0.2.12-SNAPSHOT
export PATH=$PATH:$apphome/bin:~/code/MUMmer3.23
baseoutdir=/Users/jakebiesinger
genome=rhodo
# genome=ecoli
# genome=staph
K=31
readlengths=101
numseeds=500
pipesteps1="REMOVE_BAD_COVERAGE,MERGE,DUMP_FASTA,TIP,MERGE,DUMP_FASTA,RAY_SCAFFOLD,MERGE,DUMP_FASTA,TIP,MERGE,DUMP_FASTA,RAY_SCAFFOLD,MERGE,DUMP_FASTA,TIP,MERGE,DUMP_FASTA,RAY_SCAFFOLD,MERGE,DUMP_FASTA"
pipesteps2=MERGE,DUMP_FASTA,TIP,MERGE,BUBBLE,MERGE,DUMP_FASTA
export JAVA_OPTS="-Xmx16G"
conffile=$apphome/conf/cluster.properties

# outdir=$baseoutdir/out-$genome/k31-lib1frag12-se/BL2-1kMSM3xTMSM-L3-1k-3xTMSM
outdir=$baseoutdir/out-$genome/k31-frag12-shortjump12-se/BL2-1k-M3xTMSM-repeatafter3-delayprune-true

for base in $outdir; do
    for dir in $base/*DUMP_FASTA; do 
        rm -rf *fasta* out.* *genomix-scaffolds*
        getCorrectnessStats.sh ~/in-$genome/genome.fasta $dir/genomix-scaffolds.fasta $dir/genomix-scaffolds.fasta | tee $dir/gage.txt
    done
done
