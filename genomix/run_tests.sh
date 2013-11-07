#!/bin/bash

# if the user didn't specify any specific tests to run, then run them all
if [[ -z "$UNIT_TESTS" && -z "$PIPELINE_TESTS" && -z "$RUN_LOCAL" ]]; then
    UNIT_TESTS=true
    PIPELINE_TESTS=true
    RUN_LOCAL=""
fi

echo "UNIT_TESTS: $UNIT_TESTS"
echo "PIPELINE_TESTS: $PIPELINE_TESTS"
echo "RUN_LOCAL: $RUN_LOCAL"

EXIT_CODE=0
function run_and_save_exit_code {
    eval "${cmd[@]}"
    CODE=$? && [[ $CODE != 0 ]] && EXIT_CODE=$CODE  # remember any non-zero exit codes
    echo "The command \"${cmd[@]}\" exited with $CODE"
}

if [ "$UNIT_TESTS" == "true" ]; then
    cmd=( mvn package -e -Djava.util.logging.config.file=genomix-driver/src/main/resources/conf/worker.logging.properties )
    run_and_save_exit_code
    
    ls -t */target/surefire-reports/*.txt | xargs tail -n +1  # show test reports in the order they ran
fi

if [ "$PIPELINE_TESTS" == "true" ]; then
        pushd genomix-driver/target/genomix-driver-0.2.10-SNAPSHOT
        
        # simplified pipeline
        cmd=( bin/genomix $RUN_LOCAL -kmerLength 55 -pipelineOrder BUILD_HYRACKS,MERGE,TIP_REMOVE,MERGE,BUBBLE,MERGE -localInput ../../src/test/resources/data/sequence/pathmerge/hg19.chr18.skip12K.first1K.1Kreads/ )
        run_and_save_exit_code
        
        # default pipeline
        cmd=( bin/genomix $RUN_LOCAL -kmerLength 55 -localInput ../../src/test/resources/data/sequence/pathmerge/hg19.chr18.skip12K.first1K.1Kreads/ $RUN_LOCAL )
        run_and_save_exit_code
        
        # pipeline that repeats steps
        cmd=( bin/genomix $RUN_LOCAL -kmerLength 55 -pipelineOrder BUILD_HADOOP,LOW_COVERAGE,MERGE,TIP_REMOVE,MERGE,BUBBLE,MERGE,SPLIT_REPEAT,MERGE,TIP_REMOVE,MERGE,BUBBLE,MERGE,SPLIT_REPEAT,MERGE,BUBBLE,MERGE,SPLIT_REPEAT,MERGE,BUBBLE,MERGE,SPLIT_REPEAT,MERGE -localInput ../../src/test/resources/data/sequence/pathmerge/hg19.chr18.skip12K.first1K.1Kreads/ )
        run_and_save_exit_code

        popd
fi

exit $EXIT_CODE
