package TRANS.Protocol;


import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import TRANS.OptimusInstanceID;
import TRANS.OptimusPartitionStatus;
import TRANS.Array.ArrayID;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusShapes;
import TRANS.Array.OptimusZone;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Array.ZoneID;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.Host;
import TRANS.util.TRANSWritableArray;
import TRANS.util.TransHostList;

public interface OptimusCatalogProtocol extends VersionedProtocol {
	 static public long versionID = 1; 
	 
	 public OptimusInstanceID Register(Host host);
	 public OptimusPartitionStatus heartBeat(Host host, OptimusPartitionStatus status);

	 public BooleanWritable stopCatalog();
	 public OptimusZone createZone(Text name,OptimusShape size, 
			 OptimusShape step,OptimusShapes strategy)throws WrongArgumentException;
	 public OptimusZone openZone(Text name)throws WrongArgumentException;
	 public OptimusZone openZone(ZoneID id)throws WrongArgumentException;
	 public BooleanWritable deleteZone(ZoneID id)throws WrongArgumentException;

	 public ArrayID createArray(ZoneID id,Text name,FloatWritable devalue)throws TRANS.Exceptions.WrongArgumentException;
	 public OptimusArray openArray(ZoneID id,Text name)throws TRANS.Exceptions.WrongArgumentException;
	 public OptimusArray openArray(ArrayID aid)throws TRANS.Exceptions.WrongArgumentException;
	 
	 public BooleanWritable updateArrayStatus(ArrayID aid, long uploadedSize);
	 public BooleanWritable deleteArray(ArrayID aid);
	 
	 public Host getReplicateHost(Partition p,RID rid)throws WrongArgumentException;
	 public TransHostList getHosts(Partition p)throws WrongArgumentException;
	 public BooleanWritable CreatePartition(Partition p)throws WrongArgumentException;
	 
	 public Text ListZone(Text name);
	 public Text listArray(ArrayID aid);
}
