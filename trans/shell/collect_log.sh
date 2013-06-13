#/bin/bash

dst_dir_name=$1

OPTIMUS_CONF=$OPTIMUS_CONF
OPTIMUS_HOME=$OPTIMUS_HOME

if [ "$OPTIMUS_HOME" = "" ]; then
	echo "OPTIMUS_HOME NOT SET!"
	exit -1
fi

if [ "$OPTIMUS_CONF" = "" ];then
  OPTIMUS_CONF=${OPTIMUS_HOME}/conf
	
fi

if [ "$dst_dir_name" = "" ]; then
	echo "Please specify the dest directory"
	exit
fi


MY_HOST=`hostname`

if [ "$ALLNODE_FILE" = "" ]; then
	ALLNODE_FILE=${OPTIMUS_CONF}/allnodes 
fi

for line in `cat ${ALLNODE_FILE}`
do
	mkdir -p $dst_dir_name 
	scp -r $line:$OPTIMUS_HOME/log/ $dst_dir_name 
done
