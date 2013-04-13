GENOMIX_PATH=`pwd`

for i in `cat conf/slaves`
do
   ssh $i "cd ${GENOMIX_PATH}; bin/startnc.sh"
done
