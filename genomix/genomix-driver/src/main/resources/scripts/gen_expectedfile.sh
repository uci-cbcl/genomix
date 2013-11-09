#!/bin/bash
set -e

# generate expected file 
genomix_home="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"

cd "$genomix_home"
#cd "../../../genomix-pregelix/src/test/resources/TestSet/expected/Reads/"
#echo "`pwd -P`"

input="../../../genomix-pregelix/src/test/resources/TestSet/Reads/"
output="../../../genomix-pregelix/src/test/resources/TestSet/expected/"

rm -rf $output
mkdir -p $output

cd "$input"
for pattern in MERGE; do
        echo "Patern: $pattern"
	pushd $pattern
	for file in *; do
		echo "$file"
		$genomix_home/bin/genomix -kmerLength 3 -localInput $file -pipelineOrder BUILD,$pattern -localOutput $output/$pattern/$file -randomSeed 500 
	done
	popd
done

#cmd=(bin/genomix -kmerLength 3 bin/genomix -kmerLength 3 )

