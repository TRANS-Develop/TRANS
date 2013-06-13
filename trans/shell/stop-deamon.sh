#/usr/bin
usage="$0 name"

NAME=$1

if [ "$NAME" = "" ];then
	echo $usage;
elif [ "$NAME" = "datanode" ];then
	NAME="OptimusNode"
elif [ "$NAME" = "catalog" ];then
	NAME="OptimusCatalog"
else 
	echo "Wrong argument"
	exit 0
fi
ID=`ps aux | grep $NAME | grep -v grep | awk '{print $2}'`
MY_HOST=`hostname`

if [ "$ID" = "" ]; then
	echo "$MY_HOST no $NAME running"
	exit 0
fi
echo "$MY_HOST $NAME running as $ID" 

kill $ID
sleep 1

NID=`ps aux | grep $NAME | grep -v grep | awk '{print $2}'`
echo $NID

if [ "$NID" != "" ]; then
	echo "$MY_HOST kill $NAME failure"
	exit -1
fi

echo "$MY_HOST $NAME killed"
