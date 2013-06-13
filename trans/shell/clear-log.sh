#/bin/bash

if [ "$OPTIMUS_CONF" =  "" ]; then
	echo "Using default config"
	OPTIMUS_CONF=${OPTIMUS_HOME}/conf
fi	
export OPTIMUS_CONF


SLAVE_FILE=${OPTIMUS_CONF}/datanode
for line in `cat $SLAVE_FILE`
do
	ssh $line "export OPTIMUS_HOME=$OPTIMUS_HOME;export OPTIMUS_CONF=$OPTIMUS_CONF; rm -rf $OPTIMUS_HOME/log/*;"
done
