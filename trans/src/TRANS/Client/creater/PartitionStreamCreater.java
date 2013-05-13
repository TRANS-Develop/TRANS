package TRANS.Client.creater;

import java.io.IOException;

import TRANS.OptimusReplicationManager;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.ByteWriter;
import TRANS.util.Host;
import TRANS.util.OptimusDouble2ByteStreamWriter;

public class PartitionStreamCreater extends PartitionCreater {

	private Partition p = null;
	private OptimusZone zone = null;
	OptimusCatalogProtocol ci = null;
	ByteWriter writer = null;
	private int [] srcShape = null;
	public PartitionStreamCreater(OptimusCatalogProtocol ci,Partition p,OptimusZone zone,int []srcShape)
	{
		this.p = p;
		this.zone = zone;
		this.ci = ci;
		this.srcShape = srcShape;
	}

	@Override
	public ByteWriter getWriter() throws WrongArgumentException {
		
		int relicateSize = zone.getStrategy().getShapes().size() - 1;
		Host host = ci.getReplicateHost(p,new RID(relicateSize - 1));
		

		try {
			host.ConnectReplicate();
			cout = host.getReplicateWriter();
			cin = host.getReplicateReply();
			
			cout.writeInt( relicateSize );
			p.write(cout);
			new OptimusShape(this.srcShape).write(cout);
			
			writer = new OptimusDouble2ByteStreamWriter(1024*1024,cout);
			
			
		} catch (Exception e) {

			e.printStackTrace();
			return null;
		}
		return writer;
	}
	@Override
	public boolean close() {
		// TODO Auto-generated method stub
		boolean fail = true;
		try {
			this.writer.close();
			if( OptimusReplicationManager.REPLICATE_OK == cin.readInt() )
			{
				fail = false;
			}
			cout.close();
			cin.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return fail;
	}
}
