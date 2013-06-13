package TRANS.Array;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Writable;

import TRANS.OptimusNode;
import TRANS.OptimusReplicationManager;
import TRANS.Data.TransDataType;
import TRANS.Data.Reader.Byte2DoubleReader;
import TRANS.Data.Reader.ByteReader;
import TRANS.Data.Reader.TransByteReaderFactory;
import TRANS.Data.Writer.OptimusDouble2ByteRandomWriter;
import TRANS.Data.Writer.TransWriterFactory;
import TRANS.Data.Writer.Interface.ByteWriter;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.Host;
import TRANS.util.OptimusTranslator;

public class Partition implements Writable, Runnable {
	static Log log = OptimusNode.LOG;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((arrayid == null) ? 0 : arrayid.hashCode());
		result = prime * result + ((pid == null) ? 0 : pid.hashCode());
		result = prime * result + ((rid == null) ? 0 : rid.hashCode());
		result = prime * result + ((zid == null) ? 0 : zid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Partition other = (Partition) obj;
		if (arrayid == null) {
			if (other.arrayid != null)
				return false;
		} else if (!arrayid.equals(other.arrayid))
			return false;
		if (pid == null) {
			if (other.pid != null)
				return false;
		} else if (!pid.equals(other.pid))
			return false;
		if (rid == null) {
			if (other.rid != null)
				return false;
		} else if (!rid.equals(other.rid))
			return false;
		if (zid == null) {
			if (other.zid != null)
				return false;
		} else if (!zid.equals(other.zid))
			return false;
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	private ArrayID arrayid = null;

	DataInputStream cin = null;

	DataOutputStream cout = null;

	RandomAccessFile dataf = null;

	private DataChunk localChunk = null;

	private PID pid = null;// UUID.randomUUID();
	private RID rid = null; // replication number
	private OptimusReplicationManager rmanager = null;
	private int size;
	Socket socket = null;

	private ZoneID zid = null;

	public Partition() {
	}

	public Partition(OptimusReplicationManager rmanager) {
		this.rmanager = rmanager;
	}
	public Partition(OptimusReplicationManager rmanager, DataInputStream cin,
			DataOutputStream cout, Socket socket) throws IOException,
			WrongArgumentException {

		if (rmanager == null) {
			System.out.println("Internal error rmanager null");
		}
		this.rmanager = rmanager;
		this.cin = cin;
		this.cout = cout;
		this.socket = socket;

	}
	public Partition(ZoneID zid, ArrayID aid, PID pid, RID rid)
	{
		this.zid = zid;
		this.arrayid = aid;
		this.pid = pid;
		this.rid = rid;
	}
	public Partition(OptimusReplicationManager rmanager, ZoneID zid,
			ArrayID arrayid, PID partitionid, RID rid) {
		this.zid = zid;
		this.arrayid = arrayid;
		this.pid = partitionid;
		this.rid = rid;

		OptimusZone zone = rmanager.getZone(zid);
		this.localChunk = new DataChunk(zone.getPstep().getShape(), zone
				.getStrategy().getShapes().get(rid.getId()));

		this.rmanager = rmanager;

	}
	public Partition clone() {
		return new Partition(rmanager, this.zid, this.arrayid, this.pid,
				this.rid);
	}

	// ���ص�chunk����

	public void close() throws IOException {
		if (!this.isOpened()) {
			return;
		}
		this.dataf.close();
		this.dataf = null;
	}

	public ArrayID getArrayid() {
		return arrayid;
	}

	public RandomAccessFile getDataf() {
		return dataf;
	}

	public DataChunk getLocalChunk() {
		return localChunk;
	}

	public String getPartitionPath(String home) {
		String ret = "/";
		int l = this.zid.getId() + this.arrayid.getArrayId() + this.pid.getId() + this.rid.getId();
		while(l>100)
		{
			ret+=l%100;
			l/=100;
			String dir = home+ret;
			File f = new File(dir);
			if(!f.exists())
			{
				f.mkdir();
			}
			
		}
		
		return ret+"/"+this.zid.getId() + "_" + this.arrayid.getArrayId() + "_"
				+ this.pid.getId() + "_" + this.rid.getId() + ".data";
	}

	public PID getPid() {
		return pid;
	}

	public RID getRid() {
		return rid;
	}

	public OptimusReplicationManager getRmanager() {
		return rmanager;
	}

	public int getSize() {
		return size;
	}

	public ZoneID getZid() {
		return zid;
	}

	private boolean isOpened() {
		return (this.dataf != null );
	}

	private void lastReplica(OptimusReplicationManager rmanager,
			DataInputStream cin, DataOutputStream cout) throws IOException, InterruptedException {

		this.readFields(cin);
		OptimusShape psize = new OptimusShape();
		psize.readFields(cin);
		OptimusShape src = new OptimusShape();
		src.readFields(cin);
		
		OptimusArray array = this.rmanager.getArray(this.arrayid);
		
		this.rmanager = rmanager;
		rmanager.createPatitionDataFile(this);
		OptimusZone zone = rmanager.getZone(this.zid);

		Vector<int[]> shapes = zone.getStrategy().getShapes();
		DataChunk dchunk = new DataChunk(psize.getShape(),
				shapes.get(this.getRid().getId()));
		DataChunk schunk = new DataChunk(psize.getShape(),
				src.getShape());

		
		int[] vsize = psize.getShape();
		int len = 1;
		for (int i = 0; i < vsize.length; i++) {
			len *= vsize[i];
		}

		if (schunk.ShapeEquals(dchunk)) {
			len *= array.getType().getElementSize();
			byte[] tmp = new byte[4096];
			while (len > 0) {
				int tlen = cin.read(tmp);
				len -= tlen;
				this.writeData(tmp, 0, tlen);
			}
		} else {
				
			int fnum = 0;
			ByteReader reader = TransByteReaderFactory.getByteReader(TransDataType.getClass(array.getType()), 1*1024*1024, null, cin);
			ByteWriter w = TransWriterFactory.getRandomWriter(TransDataType.getClass(array.getType()), 1024*1024, this.dataf, this);
			
			Object [] tdouble = null;
			
			
			OptimusTranslator trans = new OptimusTranslator(len, schunk, dchunk,w);		
			trans.start();

			while(fnum < len)
			{
				reader.readFromin();
				tdouble = reader.readData();
				if(tdouble == null)
				{
					System.out.println("Unexpected EOF!:fnum"+fnum+" len:"+len);
					continue;
				}
				trans.write(tdouble);
				fnum+=tdouble.length;
			}
			
			
		}
		cout.writeInt(OptimusReplicationManager.REPLICATE_OK);
		cin.close();
		cout.close();
		

	}

	private void middleReplica(OptimusReplicationManager rmanager, int rest,
			DataInputStream cin, DataOutputStream cout) throws IOException, InterruptedException, WrongArgumentException {
		
		this.readFields(cin);
		OptimusShape psize = new OptimusShape();
		psize.readFields(cin);
		
		OptimusArray array = this.rmanager.getArray(this.arrayid);
		this.rmanager = rmanager;
		// this.rid.setId(rest);

		OptimusShape src = new OptimusShape();
		src.readFields(cin);
		
		OptimusZone zone = rmanager.getZone(this.zid);
		Vector<int[]> shapes = zone.getStrategy().getShapes();
		DataChunk dchunk = new DataChunk(psize.getShape(),
				shapes.get(rest));
		DataChunk schunk = new DataChunk(psize.getShape(),
				src.getShape());

		Host h = rmanager.getReplicateHost(this, rest - 1);

		h.ConnectReplicate();
		DataOutputStream nhostOut = h.getReplicateWriter();
		DataInputStream nhostIn = h.getReplicateReply();

		nhostOut.writeInt(rest);

		Partition p = new Partition(this.rmanager, this.zid, this.arrayid,
				this.pid, new RID(this.rid.getId() - 1));
		p.write(nhostOut);
		psize.write(nhostOut);
		src.write(nhostOut);
		this.rmanager.createPatitionDataFile(this);

		int[] vsize = psize.getShape();
		int len = 1;
		for (int i = 0; i < vsize.length; i++) {
			len *= vsize[i];
		}
		
		
		
		if (schunk.ShapeEquals(dchunk)) {
			
			len *= array.getType().getElementSize();
			//this.writeMeta(nhostOut);
			byte[] tmp = new byte[4096];
			while (len > 0) {
				int tlen = cin.read(tmp);
				len -= tlen;
				nhostOut.write(tmp, 0, tlen);
				this.writeData(tmp, 0, tlen);
			}
			
		} else {
			//Byte2DoubleReader reader = new Byte2DoubleReader(1*1024*1024,nhostOut,cin);
			ByteReader reader = TransByteReaderFactory.getByteReader(TransDataType.getClass(array.getType()), 1*1024*1024, nhostOut, cin);
			ByteWriter w = TransWriterFactory.getRandomWriter(TransDataType.getClass(array.getType()), 1024*1024, this.dataf, this);
			int fnum = 0;
			
			Object [] tdouble = null;
			//ByteWriter  w = new OptimusDouble2ByteRandomWriter(1024*1024,this.dataf,this);
			OptimusTranslator trans = new OptimusTranslator(len, schunk, dchunk,w);
		
			//OptimusWriter writer = new OptimusWriter(w,len*8);
			trans.start();
			//writer.start();

			while(fnum < len)
			{
				reader.readFromin();
				tdouble = reader.readData();
				if(tdouble == null)
				{
					System.out.println("Unexpected EOF!:fnum"+fnum+" len:"+len);
					continue;
				}
				trans.write(tdouble);
				fnum += tdouble.length;
				
			}
			
		}
		long b = System.currentTimeMillis();
		int ret = nhostIn.readInt();
		System.out.println("Waiting time:" + ( System.currentTimeMillis() - b));
		
		cout.writeInt(ret);
		cin.close();
		cout.close();
		nhostIn.close();
		nhostOut.close();

	}

	public void open() throws IOException {
		if (this.isOpened()) {
			return;
		}
		this.rmanager.openPatitionDataFile(this);

	}

	public int read(byte[] buffer) throws IOException {
		return this.dataf.read(buffer);
	}

	public Object[] read(DataChunk chunk) throws IOException {
		Object[] data = new Object[chunk.getSize()];
		OptimusArray array = this.rmanager.getArray(this.arrayid);
		System.out.println(chunk.getChunkOffset() * this.rmanager.getArray(this.arrayid).getType().getElementSize());
		dataf.seek(chunk.getChunkOffset() * array.getType().getElementSize());
		/*	for (int i = 0; i < chunk.getSize(); i++) {
			data[i] = this.dataf.readDouble();
		}
		*/
		int fnum = 0;
		ByteReader reader = TransByteReaderFactory.getByteReader(TransDataType.getClass(array.getType()), 1*1024*1024, null, dataf); 
		//		new Byte2DoubleReader(1*1024*1024,null,dataf);
		Object [] tdouble = null;
		int size = chunk.getSize();
		while(fnum < size)
		{
			reader.readFromin(size - fnum);
			tdouble = reader.readData();
			if(tdouble == null) return data;
			for(int i = 0; i < tdouble.length; i++)
			{
				data[fnum++] = tdouble[i];
			}
		}
		
		return data;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		this.zid = new ZoneID();
		this.zid.readFields(in);
		this.arrayid = new ArrayID();
		this.arrayid.readFields(in);
		this.pid = new PID();
		this.pid.readFields(in);
		this.rid = new RID();
		this.rid.readFields(in);
	}

	public void readMeta(DataInputStream fin) throws IOException {
		if (fin == null) {
			throw new IOException();
		}
		this.zid = new ZoneID();
		this.zid.readFields(fin);

		this.arrayid = new ArrayID();
		arrayid.readFields(fin);

		this.pid = new PID(0);
		this.pid.readFields(fin);

		this.rid = new RID(0);
		this.rid.readFields(fin);

	}

	@Override
	public void run() {
		int rnum;
		try {
			rnum = cin.readInt();
			System.out.println("Rnum:"+rnum);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} // the number of replication to create

		if (rnum == 0) {
			return;
		}
		try {
			if (rnum == 1) {
				lastReplica(rmanager, cin, cout);
			} else {
				middleReplica(rmanager, rnum - 1, cin, cout);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			socket.close();
			///this.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.rid = new RID(rnum-1);
		this.rmanager.addPartition(this);
	}

	public void setArrayid(ArrayID arrayid) {
		this.arrayid = arrayid;
	}

	public void setDataf(RandomAccessFile dataf) {
		this.dataf = dataf;
	}

	public void setLocalChunk(DataChunk localChunk) {
		this.localChunk = localChunk;
	}

	public void setPid(PID pid) {
		this.pid = pid;
	}

	public void setRid(RID rid) {
		this.rid = rid;
	}

	public void setRmanager(OptimusReplicationManager rmanager) {
		this.rmanager = rmanager;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public void setZid(ZoneID zid) {
		this.zid = zid;
	}

	@Override
	public String toString() {
		return "Partition [arrayid=" + arrayid + ", pid=" + pid + ", rid="
				+ rid + ", zid=" + zid + "]";
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.zid.write(out);
		this.arrayid.write(out);
		this.pid.write(out);
		this.rid.write(out);

	}

	public void writeData(byte[] b, int off, int len) throws IOException {
		this.dataf.write(b, off, len);
	}

	public void writeMeta(DataOutputStream fout) throws IOException {
		if (fout == null) {
			throw new IOException();
		}
		zid.write(fout);
		arrayid.write(fout);
		pid.write(fout);
		this.rid.write(fout);
		
	}
	
}
