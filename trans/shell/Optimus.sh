#/bin/bash

OPTIMUS_HOME=${OPTIMUS_HOME}
OPTIMUS_CONF=${OPTIMUS_CONF}

JAVA_CMD=java
CMD=$1
shift
args=$@


#find class path
for f in $OPTIMUS_HOME/*.jar; do
	CLASSPATH=${CLASSPATH}:$f;
done

for f in $OPTIMUS_HOME/lib/*jar; do
	CLASSPATH=${CLASSPATH}:$f;
done

if [ "$CMD" = "datanode" ];then
	CLASS='TRANS.OptimusNode'
elif [ "$CMD" = "catalog" ]; then
	CLASS='TRANS.OptimusCatalog'
elif [ "$CMD" = "clienttest" ]; then
	CLASS='TRANS.ClientTest'
elif [ "$CMD" = "readtest" ] ; then 
	CLASS='TRANS.ReaderTest'
elif [ "$CMD" = "reader" ] ; then 
	CLASS='TRANS.Client.Reader.OptimusReader'
elif [ "$CMD" = "netcdfloader" ]; then
	CLASS='TRANS.Client.NetcdfLoader'
elif [ "$CMD" = "rcreate" ]; then
	CLASS='TRANS.Client.creater.RandomArrayCreater' 
fi

if [ "$OPTIMUS_CONF" = "" ]; then
	OPTIMUS_CONF=${OPTIMUS_HOME}/conf
fi
exec "${JAVA_CMD}" -classpath "$CLASSPATH" $CLASS $args -c $OPTIMUS_CONF 
