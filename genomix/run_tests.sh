#!/bin/bash

if [ -z "$UNIT_TESTS" ]; then UNIT_TESTS=true; fi
if [ -z "$PIPELINE_TESTS" ]; then PIPELINE_TESTS=true; fi
if [ -z "$RUN_LOCAL" ]; then RUN_LOCAL=""; fi

echo "UNIT_TESTS: $UNIT_TESTS"
echo "PIPELINE_TESTS: $PIPELINE_TESTS"
echo "RUN_LOCAL: $RUN_LOCAL"

EXIT_CODE=0

if [ "$UNIT_TESTS" == "true" ]; then 
    mvn package -q -e -Djava.util.logging.config.file=genomix-driver/src/main/resources/conf/worker.logging.properties
    CODE=$? && [[ $CODE != 0 ]] && EXIT_CODE=$CODE  # remember any non-zero exit codes
    ls -t */target/surefire-reports/*.txt | xargs tail -n +1  # show test reports in the order they ran
    CODE=$? && [[ $CODE != 0 ]] && EXIT_CODE=$CODE
fi

if [ "$PIPELINE_TESTS" == "true" ]; then
        pushd genomix-driver/target/genomix-driver-0.2.10-SNAPSHOT
        
        # simplified pipeline
        bin/genomix $RUN_LOCAL -kmerLength 55 -pipelineOrder BUILD_HYRACKS,MERGE,TIP_REMOVE,MERGE,BUBBLE,MERGE -localInput ../../src/test/resources/data/sequence/pathmerge/hg19.chr18.skip12K.first1K.1Kreads/
        CODE=$? && [[ $CODE != 0 ]] && EXIT_CODE=$CODE
        
        # default pipeline
        bin/genomix $RUN_LOCAL -kmerLength 55 -localInput ../../src/test/resources/data/sequence/pathmerge/hg19.chr18.skip12K.first1K.1Kreads/ $RUN_LOCAL
        CODE=$? && [[ $CODE != 0 ]] && EXIT_CODE=$CODE
        
        # pipeline that repeats steps
        bin/genomix $RUN_LOCAL -kmerLength 55 -pipelineOrder BUILD_HADOOP,LOW_COVERAGE,MERGE,TIP_REMOVE,MERGE,BUBBLE,MERGE,SPLIT_REPEAT,MERGE,TIP_REMOVE,MERGE,BUBBLE,MERGE,SPLIT_REPEAT,MERGE,BUBBLE,MERGE,SPLIT_REPEAT,MERGE,BUBBLE,MERGE,SPLIT_REPEAT,MERGE -localInput ../../src/test/resources/data/sequence/pathmerge/hg19.chr18.skip12K.first1K.1Kreads/ 
        CODE=$? && [[ $CODE != 0 ]] && EXIT_CODE=$CODE

        popd
fi

exit $EXIT_CODE