#/bin/bash

if [ "$OPTIMUS_CONF" =  "" ]; then
	echo "Using default config"
	OPTIMUS_CONF=${OPTIMUS_HOME}/conf
fi	
export OPTIMUS_CONF

CATALOG_FILE=${OPTIMUS_CONF}/catalog 

for line in `cat ${CATALOG_FILE}`
do
	ssh $line "export OPTIMUS_HOME=$OPTIMUS_HOME;export OPTIMUS_CONF=$OPTIMUS_CONF;mkdir -p $OPTIMUS_HOME/log; nohup $OPTIMUS_HOME/shell/Optimus.sh catalog &> $OPTIMUS_HOME/log/${line}_catalog.log &"
	echo $line" Catalog ok!"
done

SLAVE_FILE=${OPTIMUS_CONF}/datanode
for line in `cat $SLAVE_FILE`
do
	ssh $line "export OPTIMUS_HOME=$OPTIMUS_HOME;export OPTIMUS_CONF=$OPTIMUS_CONF; mkdir -p $OPTIMUS_HOME/log; nohup $OPTIMUS_HOME/shell/Optimus.sh datanode &> $OPTIMUS_HOME/log/${line}_node.log &"
	echo $line" OptimusNode ok!"
done
