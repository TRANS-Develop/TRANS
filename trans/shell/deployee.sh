#/bin/bash


OPTIMUS_CONF=$OPTIMUS_CONF
OPTIMUS_HOME=$OPTIMUS_HOME
dir=$1
dst=$2

if [ "$OPTIMUS_HOME" = "" ]; then
	echo "OPTIMUS_HOME NOT SET!"
	exit -1
fi

if [ "$OPTIMUS_CONF" =  "" ]; then
	echo "Using default config"
	OPTIMUS_CONF=${OPTIMUS_HOME}/conf
fi	
export OPTIMUS_CONF

MY_HOST=`hostname`

if [ "$ALLNODE_FILE" = "" ]; then
	ALLNODE_FILE=${OPTIMUS_CONF}/allnodes 
fi

for line in `cat ${ALLNODE_FILE}`
do
	if [ "$line" != "$MY_HOST" ]; then
		echo $line
		scp -r ${dir} $line:$OPTIMUS_HOME/$2

	fi
done
