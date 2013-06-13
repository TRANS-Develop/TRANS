#/bin/bash



if [ "$OPTIMUS_CONF" =  "" ]; then
	echo "Using default config"
	OPTIMUS_CONF=${OPTIMUS_HOME}/conf
fi	
export OPTIMUS_CONF

CATALOG_FILE=${OPTIMUS_CONF}/catalog 

for line in `cat ${CATALOG_FILE}`
do
	ssh $line "export OPTIMUS_HOME=$OPTIMUS_HOME;export OPTIMUS_CONF=$OPTIMUS_CONF;mkdir -p $OPTIMUS_HOME/log; $OPTIMUS_HOME/shell/stop-deamon.sh catalog "
done

SLAVE_FILE=${OPTIMUS_CONF}/datanode
for line in `cat $SLAVE_FILE`
do
	ssh $line "export OPTIMUS_HOME=$OPTIMUS_HOME;export OPTIMUS_CONF=$OPTIMUS_CONF;mkdir -p $OPTIMUS_HOME/log; $OPTIMUS_HOME/shell/stop-deamon.sh datanode "
done
