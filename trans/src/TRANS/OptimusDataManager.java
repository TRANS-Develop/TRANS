package TRANS;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import TRANS.Array.ArrayID;
import TRANS.Array.DataChunk;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.PartitialCreateResult;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Calculator.OptimusCalculator;
import TRANS.Client.creater.PartitionStreamCreater;
import TRANS.Data.Optimus1Ddata;
import TRANS.Data.OptimusData;
import TRANS.Data.TransDataType;
import TRANS.Data.Writer.OptimusDouble2ByteRandomWriter;
import TRANS.Data.Writer.TransWriterFactory;
import TRANS.Data.Writer.Interface.ByteWriter;
import TRANS.MR.Average.AverageResultArrayWritable;
import TRANS.MR.Average.StrideAverageResult;
import TRANS.MR.Median.StrideResult;
import TRANS.MR.Median.MedianResultArrayWritable;
import TRANS.MR.Median.StripeMedianResult;
import TRANS.MR.io.AverageResult;
import TRANS.Protocol.OptimusCalculatorProtocol;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusTranslator;
import TRANS.util.TRANSDataIterator;

/*
 * serve data to the client or the other component of the cluster
 */
public class OptimusDataManager extends Thread implements OptimusDataProtocol,
		OptimusCalculatorProtocol {

	/**
	 * @param start
	 *            : Ҫ����λ�õĿ�ʼλ��
	 * @param off
	 *            �� Ҫ�����ݵ�off
	 * @param fsize
	 *            : src �������С
	 * @param tsize
	 *            : Ŀ���ڴ�������С
	 * @param fdata
	 *            : src�����
	 * @param fstart
	 *            : src �������ʼλ��
	 * @param tdata
	 *            �� Ŀ���ڴ�
	 * @param tstart
	 *            : Ŀ���ڴ��start
	 * @return
	 */
	public static int readFromMem(int start[], int[] off, int[] fsize,
			int[] tsize, Object[] fdata, int[] fstart, Object[] tdata,
			int[] tstart) {
		int size = 1;
		int fpos = 0;
		int tpos = 0;
		int[] fjump = new int[start.length];
		int[] djump = new int[start.length];
		for (int i = 0; i < start.length; i++) {
			size *= off[i];
			fpos = (fpos == 0) ? start[i] - fstart[i] : fpos * fsize[i]
					+ start[i] - fstart[i];
			tpos = (tpos == 0) ? start[i] - tstart[i] : tpos * tsize[i]
					+ start[i] - tstart[i];

		}
		fjump[start.length - 1] = fsize[start.length - 1];
		djump[start.length - 1] = tsize[start.length - 1];
		for (int i = start.length - 2; i >= 0; i--) {
			fjump[i] = fsize[i] * fjump[i + 1];
			djump[i] = tsize[i] * djump[i + 1];
		}
		if (size == 0) {
			return 0;
		}

		int len = start.length - 1;
		int[] iter = new int[len + 1];

		int j = 0;
		while (iter[0] < off[0]) {
			for (int i = 0; i < off[len]; i++) {
				tdata[tpos + i] = fdata[fpos + i];
			}
			j = len - 1;

			while (j >= 0) {
				iter[j]++;
				fpos += fjump[j + 1];
				tpos += djump[j + 1];
				if (iter[j] < off[j]) {
					break;
				} else if (j == 0) {
					break;
				} else {

					fpos -= iter[j] * fjump[j + 1];
					tpos -= iter[j] * djump[j + 1];

					iter[j] = 0;
				}
				j--;
			}

		}
		return size;
	}

	int dataport = 0;

	OptimusReplicationManager rmanger = null;//

	private Server server = null;
	java.util.Map<Partition, PartitialCreateResult> partitionInCreate = new java.util.HashMap<Partition, PartitialCreateResult>();

	OptimusDataManager(OptimusConfiguration conf,
			OptimusReplicationManager rmanger, int dataport, String dataDir,
			String metaDir) throws UnknownHostException, IOException {
		Configuration hconf = new Configuration();

		this.dataport = dataport;
		server = RPC.getServer(this, InetAddress.getLocalHost()
				.getHostAddress(), dataport, 10, false, hconf);

		this.rmanger = rmanger;

	}

	@Override
	public OptimusData FindMaxMin(ArrayID aid, PID pid, OptimusShape pshape,
			OptimusShape start, OptimusShape off) throws Exception {
		// TODO Auto-generated method stub
		/*
		 * OptimusData data = this.readData(aid, pid, pshape, start, off);
		 * 
		 * if (data == null) return null; double[] fdata = data.getData();
		 * 
		 * double[] rminmax = new double[2]; rminmax[0] = fdata[0]; rminmax[1] =
		 * fdata[0]; for (int i = 1; i < fdata.length; i++) { if (fdata[i] >
		 * rminmax[0]) { rminmax[0] = fdata[i]; } else if (fdata[i] <
		 * rminmax[1]) { rminmax[1] = fdata[i]; } } return new
		 * Optimus1Ddata(rminmax);
		 */
		return null;
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#interrupt()
	 */
	@Override
	public void interrupt() {
		// TODO Auto-generated method stub
		this.server.stop();
		super.interrupt();
	}

	@Override
	public OptimusData JoinArray(Partition array1, Partition array2,
			OptimusShape pshape, OptimusShape rstart, OptimusShape roff,
			OptimusCalculator c) throws IOException {

		array1.setRmanager(this.rmanger);
		array2.setRmanager(this.rmanger);

		Object[] data1 = this.readData(array1.getArrayid(), array1.getPid(),
				pshape, rstart, roff).getData();
		Object[] data2 = this.readData(array2.getArrayid(), array1.getPid(),
				pshape, rstart, roff).getData();
		Object[] data3 = c.calcArray(data1, data2);

		// return new OptimusData();
		return new OptimusData(data3, rstart, roff, roff);
	}

	@Override
	public BooleanWritable putPartitionData(Partition p, TRANSDataIterator data)
			throws IOException {
		try {

			PartitialCreateResult itp = null;
			DataChunk chunk = null;
			OptimusZone zone = null;
			int size = 0;
			int[] shape = null;
			int id = p.getRid().getId();
			synchronized (partitionInCreate) {

				if (this.partitionInCreate.containsKey(p)) {
					itp = this.partitionInCreate.get(p);

				} else {
					zone = this.rmanger.getZone(p.getZid());
					int[] asize = zone.getSize().getShape();
					int[] pstep = zone.getPstep().getShape();

					chunk = new DataChunk(asize, pstep);
					int pnum = p.getPid().getId();
					DataChunk tmp = null;
					for (int i = 0; i < asize.length; i++) {
						while (chunk != null && chunk.getChunkNum() < pnum) {
							tmp = chunk;
							chunk = chunk.moveUp(i);
						}
						if (chunk == null) {
							chunk = tmp;
						}
						if (chunk.getChunkNum() == pnum) {
							break;
						} else {
							chunk.moveDown(i);
						}
						chunk = tmp;
					}
					size = chunk.getSize();
					OptimusArray array = this.rmanger.getArray(p.getArrayid());
					itp = new PartitialCreateResult(array.getType(),new Object[size],
							chunk.getStart(), chunk.getChunkSize());
					this.partitionInCreate.put(p, itp);
				}
			}
			if (!itp.AddResult(data)) {
				return new BooleanWritable(false);
			}
			if (itp.equals(data)) {
				itp.setData(data.getData());
				itp.setFull();

			} else {
				itp.init(data.getStart(), data.getShape());
				data.init(itp.getStart(), itp.getShape());
				while (itp.next() && data.next()) {
					itp.add(data.get());
				}
			}
			boolean isFull = false;
			synchronized (partitionInCreate) {
				itp = partitionInCreate.get(p);
				if (itp.isFull()) {
					partitionInCreate.remove(p);
					this.rmanger.createPatitionDataFile(p);
					isFull = true;
				}
			}
			if (isFull) {
				// ByteWriter w = new
				// OptimusDouble2ByteRandomWriter(1024*1024,p.getDataf(),p);
				OptimusArray array = this.rmanger.getArray(p.getArrayid());
				ByteWriter w = TransWriterFactory.getRandomWriter(
						TransDataType.getClass(array.getType()), 1024 * 1024,
						p.getDataf(), p);
				zone = this.rmanger.getZone(p.getZid());
				int[] asize = zone.getSize().getShape();
				int[] pstep = zone.getPstep().getShape();

				chunk = new DataChunk(asize, pstep);
				shape = zone.getStrategy().getShapes().get(id);
				DataChunk dstChunk = new DataChunk(chunk.getChunkSize(), shape);
				DataChunk srcChunk = new DataChunk(chunk.getChunkSize(),
						chunk.getChunkSize());
				size = chunk.getSize();
				OptimusTranslator trans = new OptimusTranslator(size, srcChunk,
						dstChunk, w);
				trans.start();
				trans.write(itp.getData());
				this.rmanger.addPartition(p);
			}
			BooleanWritable puted = new BooleanWritable(true);
			if (id > 0) {
				Host h = this.rmanger.getReplicateHost(p, id - 1);

				OptimusDataProtocol dp = h.getDataProtocol();
				Partition np = p.clone();
				np.setRid(new RID(id - 1));
				puted = dp.putPartitionData(np, data);

				return puted;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Wrong Operation");
		}

		return new BooleanWritable(true);

	}

	@Override
	public AverageResult readAverage(ArrayID aid, PID pid, OptimusShape pshape,
			OptimusShape starts, OptimusShape offs) throws IOException {
		OptimusData data = this.readData(aid, pid, pshape, starts, offs);
		Object[] d = data.getData();
		Double[] rdata = new Double[2];
		rdata[0] = new Double(d.length);
		AverageResult r = new AverageResult();
		r.addAll(d);
		/*
		 * for (int i = 0; i < d.length; i++) { r.addValue(d[i]); }
		 */
		return r;
	}

	@Override
	public OptimusData readData(ArrayID aid, PID pid, OptimusShape pshape,
			OptimusShape starts, OptimusShape offs) throws IOException {

		int[] start = starts.getShape();
		int[] off = offs.getShape();

		Partition p = this.rmanger.getPartitionById(aid, pid);
		if (p == null) {
			System.out.println("Wrong partion");
			return null;
		}
		System.out.println("Reading partition:" + p.toString());

		OptimusZone zone = this.rmanger.getZone(p.getZid());
		Vector<int[]> shapes = zone.getStrategy().getShapes();

		DataChunk chunk = new DataChunk(pshape.getShape(), shapes.get(p
				.getRid().getId()));

		int rsize = 1;
		for (int i = 0; i < off.length; i++)
			rsize *= off[i];
		Set<DataChunk> chunks = chunk.getAdjacentChunks(start, off);

		Object[] rdata = new Object[rsize];
		// TRANSDataIterator ritr = new TRANSDataIterator(rdata, start, off);

		p.open();
		for (DataChunk c : chunks) {

			Object[] data = p.read(c);
			int[] nstart = new int[start.length];
			int[] noff = new int[start.length];

			int[] cstart = c.getStart();
			int[] coff = c.getChunkSize();
			/*
			 * TRANSDataIterator citr = new TRANSDataIterator(data, cstart,
			 * coff); ritr.init(cstart, coff); citr.init(start, off); while
			 * (citr.next() && ritr.next()) { ritr.set(citr.get()); }
			 */

			for (int i = 0; i < start.length; i++) {
				nstart[i] = start[i] > cstart[i] ? start[i] : cstart[i];
				noff[i] = start[i] + off[i] < cstart[i] + coff[i] ? start[i]
						+ off[i] : cstart[i] + coff[i];
				noff[i] -= nstart[i];
			}
			try {
				readFromMem(nstart, noff, c.getChunkSize(), off, data,
						c.getStart(), rdata, start);
			} catch (Exception e) {
				System.out.print("Exception");
			}

		}
		
		return new OptimusData(rdata, starts, offs, offs);

	}

	@Override
	public MedianResultArrayWritable readStride(Partition p,
			OptimusShape pshape, OptimusShape start, OptimusShape off,
			OptimusShape stride) throws IOException {

		OptimusArray array = this.rmanger.getArray(p.getArrayid());
		Class<?> eleType = TransDataType.getClass(array.getType());
		StripeMedianResult[] ret = null;
		try {
			// System.out.println(p.toString());
			OptimusZone zone = this.rmanger.getZone(p.getZid());
			Vector<int[]> shapes = zone.getStrategy().getShapes();

			DataChunk chunk = new DataChunk(off.getShape(), stride.getShape());

			p = this.rmanger.getPartitionById(p.getArrayid(), p.getPid());

			int[] asize = zone.getSize().getShape();
			int[] pstep = zone.getPstep().getShape();
			// 没有考虑overlap
			DataChunk partition = new DataChunk(asize, pstep);
			int pnum = p.getPid().getId();
			DataChunk tmpar = null;
			for (int i = 0; i < asize.length; i++) {

				while (partition != null && partition.getChunkNum() < pnum) {
					tmpar = partition;
					partition = partition.moveUp(i);

				}
				if (partition == null)
					partition = tmpar;
				if (partition.getChunkNum() == pnum) {
					break;
				}
				partition = tmpar;

			}
			// 所有overlap的stride及其编号
			int[] pstart = partition.getStart();
			int[] readStart = new int[pstart.length];
			int[] readOff = new int[pstart.length];

			for (int i = 0; i < pstart.length; i++) {
				readStart[i] = pstart[i] > start.getShape()[i] ? pstart[i]
						: start.getShape()[i];
				readOff[i] = start.getShape()[i] + off.getShape()[i] < pstart[i]
						+ partition.getChunkSize()[i] ? (start.getShape()[i] + off
						.getShape()[i])
						: (pstart[i] + partition.getChunkSize()[i]);
				readOff[i] -= readStart[i];

				readStart[i] = readStart[i] - start.getShape()[i]; //
			}
			Set<DataChunk> chunks = chunk.getAdjacentChunks(readStart, readOff);
			Map<Integer, StripeMedianResult> itrs = new HashMap<Integer, StripeMedianResult>();

			for (DataChunk c : chunks) {
				StripeMedianResult itr = null;
				int[] gstart = new int[pstart.length];
				for (int i = 0; i < pstart.length; i++) {
					gstart[i] = start.getShape()[i] + c.getStart()[i];
				}
				if (eleType.equals(Double.class)) {
					itr = new StripeMedianResult<Double>(c.getChunkNum(),
							gstart, stride.getShape(), eleType);
				} else if (eleType.equals(Float.class)) {
					itr = new StripeMedianResult<Float>(c.getChunkNum(),
							gstart, stride.getShape(), eleType);

				} else if (eleType.equals(Integer.class)) {
					itr = new StripeMedianResult<Float>(c.getChunkNum(),
							gstart, stride.getShape(), eleType);

				} else {
					System.out.println("UnSupported type@readStride");
					throw new IOException("UnSupported type@readStride");
				}
				itrs.put(c.getChunkNum(), itr);
			}

			DataChunk c = new DataChunk(pshape.getShape(), shapes.get(p
					.getRid().getId()));

			for (int i = 0; i < pstart.length; i++) {
				readStart[i] = pstart[i] > start.getShape()[i] ? pstart[i]
						: start.getShape()[i];
				readOff[i] = start.getShape()[i] + off.getShape()[i] < pstart[i]
						+ partition.getChunkSize()[i] ? (start.getShape()[i] + off
						.getShape()[i])
						: (pstart[i] + partition.getChunkSize()[i]);
				readOff[i] -= readStart[i];

				readStart[i] = readStart[i] - pstart[i]; //
			}
			Set<DataChunk> ts = c.getAdjacentChunks(readStart, readOff);

			DataChunk[] tiles = new DataChunk[1];
			tiles = ts.toArray(tiles);
			Arrays.sort(tiles);
			// p.setRmanager(this.rmanger);

			p.open();

			for (DataChunk cc : tiles) {
				Object[] data = p.read(cc);
				int[] globalRange = new int[asize.length];
				int[] cstart = cc.getStart();
				for (int i = 0; i < asize.length; i++) {
					globalRange[i] = cstart[i] + pstart[i];
				}

				StrideResult it = new StrideResult(new TransDataType(eleType),data, globalRange,
						cc.getChunkSize());
				for (Map.Entry i : itrs.entrySet()) {
					StripeMedianResult tmp = (StripeMedianResult) i.getValue();

					if (!tmp.isOverlaped(it.getStart(), it.getShape())) {
						continue;
						// throw new IOException("Wrong operation");
					}

					it.init(tmp.getStart(), stride.getShape());
					if (eleType.equals(Double.class)) {
						while (it.next()) {
							tmp.add((Double) it.get());
						}
					} else if (eleType.equals(Float.class)) {
						while (it.next()) {

							tmp.add((Float) it.get());
						}
					} else if (eleType.equals(Integer.class)) {
						while (it.next()) {
							tmp.add((Integer) it.get());
						}
					} else {
						System.out.println("UnSupported type@readStride");
						throw new IOException("UnSupported type@readStride");
					}
				}
			}
			ret = new StripeMedianResult[itrs.size()];
			int s = 0;
			for (Map.Entry i : itrs.entrySet()) {
				StripeMedianResult r = (StripeMedianResult) i.getValue();
				r.setId((Integer) i.getKey());
				ret[s++] = r;
			}
		} catch (Exception e) {
			throw new IOException(e.toString());
		}
		return new MedianResultArrayWritable(ret);
	}

	public AverageResultArrayWritable readStrideAverage(Partition p,
			OptimusShape pshape, OptimusShape start, OptimusShape off,
			OptimusShape stride) throws IOException {
		// System.out.println(p.toString());
		OptimusZone zone = this.rmanger.getZone(p.getZid());
		Vector<int[]> shapes = zone.getStrategy().getShapes();

		OptimusArray array = this.rmanger.getArray(p.getArrayid());
		Class<?> eleType = TransDataType.getClass(array.getType());

		DataChunk chunk = new DataChunk(off.getShape(), stride.getShape());

		p = this.rmanger.getPartitionById(p.getArrayid(), p.getPid());

		int[] asize = zone.getSize().getShape();
		int[] pstep = zone.getPstep().getShape();
		// 没有考虑overlap
		DataChunk partition = new DataChunk(asize, pstep);
		int pnum = p.getPid().getId();
		DataChunk tmpar = null;
		for (int i = 0; i < asize.length; i++) {

			while (partition != null && partition.getChunkNum() < pnum) {
				tmpar = partition;
				partition = partition.moveUp(i);

			}
			if (partition == null)
				partition = tmpar;
			if (partition.getChunkNum() == pnum) {
				break;
			}
			partition = tmpar;

		}
		// 所有overlap的stride及其编号
		int[] pstart = partition.getStart();
		int[] readStart = new int[pstart.length];
		int[] readOff = new int[pstart.length];

		for (int i = 0; i < pstart.length; i++) {
			readStart[i] = pstart[i] > start.getShape()[i] ? pstart[i] : start
					.getShape()[i];
			readOff[i] = readStart[i] + off.getShape()[i] < pstart[i]
					+ partition.getChunkSize()[i] ? readStart[i]
					+ off.getShape()[i] : pstart[i]
					+ partition.getChunkSize()[i];
			readOff[i] -= readStart[i];

			readStart[i] = readStart[i] - start.getShape()[i]; //
		}

		Set<DataChunk> chunks = chunk.getAdjacentChunks(readStart, readOff);
		Map<Integer, StrideAverageResult> itrs = new HashMap<Integer, StrideAverageResult>();

		for (DataChunk c : chunks) {
			int[] gstart = new int[pstart.length];
			for (int i = 0; i < pstart.length; i++) {
				gstart[i] = start.getShape()[i] + c.getStart()[i];
			}
			StrideAverageResult itr = new StrideAverageResult(eleType,c.getChunkNum(),
					gstart, stride.getShape());
			itrs.put(c.getChunkNum(), itr);
		}

		DataChunk c = new DataChunk(pshape.getShape(), shapes.get(p.getRid()
				.getId()));

		// int []readStart = new int[pstart.length];
		// int []readOff = new int[pstart.length];

		for (int i = 0; i < pstart.length; i++) {
			readStart[i] = pstart[i] > start.getShape()[i] ? pstart[i] : start
					.getShape()[i];
			readOff[i] = readStart[i] + off.getShape()[i] < pstart[i]
					+ partition.getChunkSize()[i] ? readStart[i]
					+ off.getShape()[i] : pstart[i]
					+ partition.getChunkSize()[i];
			readOff[i] -= readStart[i];

			readStart[i] = readStart[i] - pstart[i]; //
		}
		Set<DataChunk> ts = c.getAdjacentChunks(readStart, readOff);

		DataChunk[] tiles = new DataChunk[1];
		tiles = ts.toArray(tiles);
		Arrays.sort(tiles);
		// p.setRmanager(this.rmanger);

		p.open();

		for (DataChunk cc : tiles) {
			Object[] data = p.read(cc);
			int[] globalRange = new int[asize.length];
			int[] cstart = cc.getStart();
			for (int i = 0; i < asize.length; i++) {
				globalRange[i] = cstart[i] + pstart[i];
			}

			StrideResult it = new StrideResult(new TransDataType(eleType),data, globalRange,
					cc.getChunkSize());
			for (Map.Entry i : itrs.entrySet()) {
				StrideAverageResult tmp = (StrideAverageResult) i.getValue();

				if (!tmp.isOverlaped(it.getStart(), it.getShape())) {
					continue;
				}

				it.init(tmp.getStart(), stride.getShape());
				if (eleType.equals(Double.class)) {
					while (it.next()) {
						tmp.addValue(((Double) it.get()));
					}
				} else if (eleType.equals(Float.class)) {
					while (it.next()) {
						tmp.addValue((Float) it.get());
					}
				} else if (eleType.equals(Integer.class)) {
					while (it.next()) {
						tmp.addValue((Integer) it.get());
					}
				} else {
					System.out.println("UnSupported type@readStrideAverage");
					throw new IOException("UnSupported type@readStrideAverage");
				}
				/*
				 * while(it.next()) { tmp.addValue(it.get()); }
				 */
			}
		}
		StrideAverageResult[] ret = new StrideAverageResult[itrs.size()];
		int s = 0;
		for (Map.Entry i : itrs.entrySet()) {
			StrideAverageResult r = (StrideAverageResult) i.getValue();
			r.setId((Integer) i.getKey());

			ret[s++] = r;

		}
		return new AverageResultArrayWritable(ret);

	}

	/*
	 * Host, the host to hold the recovered partition
	 */
	@Override
	public IntWritable RecoverPartition(Partition p, OptimusShape psize,
			Host host) {

		return this.RecoverReadAll(host, psize, p, p.getRid());

	}

	@Override
	public IntWritable RecoverReadAll(Host h, OptimusShape psize, Partition p,
			RID rid) {
		// TODO Auto-generated method stub
		int ret = 0;
		try {
			System.out.println("Recover from host" + h);
			h.ConnectReplicate();
			DataOutputStream cout = h.getReplicateWriter();
			DataInputStream cin = h.getReplicateReply();

			cout.writeInt(1);// only 1 relication to write
			Partition src = rmanger
					.getPartitionById(p.getArrayid(), p.getPid());
			Partition dst = src.clone();

			OptimusZone zone = rmanger.getZone(p.getZid());
			OptimusShape srcshape = new OptimusShape(zone.getStrategy()
					.getShapes().get(src.getRid().getId()));
			dst.setRid(rid);

			dst.writeMeta(cout);
			psize.write(cout);
			srcshape.write(cout);
			System.out.println(dst);
			System.out.println(psize);
			System.out.println(srcshape);
			// src.writeMeta(cout);
			int tlen;
			src.open();
			byte[] data = new byte[4096];
			while ((tlen = src.read(data)) > 0) {
				cout.write(data, 0, tlen);
			}
			ret = cin.readInt();

		} catch (IOException e) {
			e.printStackTrace();
			return new IntWritable(OptimusReplicationManager.REPLICATE_FAILURE);
		}
		return new IntWritable(ret);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#start()
	 */
	@Override
	public synchronized void start() {
		this.server.start();
	}

}
