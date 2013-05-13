package TRANS.MR.Binary;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.jdom2.JDOMException;

import TRANS.Array.ArrayID;
import TRANS.Array.DataChunk;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Array.ZoneID;
import TRANS.Calculator.OptimusCalculator;
import TRANS.Calculator.OptimusDoubleCalculator;
import TRANS.Client.ZoneClient;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.MR.TRANSInputSplit;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.OptimusConfiguration;
import TRANS.util.TransHostList;
import TRANS.util.UTILS;

public class TransBinaryInputFormat extends
		FileInputFormat<Object, TransBinaryMapInputValue> {
	

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		JobConf conf = (JobConf) job.getConfiguration();
		String tmp = null;
		String[] tmp2 = null;
		tmp = conf.get("TRANS.INPUT.ARG1.NAME");
		System.out.println("Arg1:"+tmp);
		tmp2 = tmp.split("\\.");
		String zone1 = tmp2[0];
		String name1 = tmp2[1];

		tmp = conf.get("TRANS.INPUT.ARG2.NAME");
		System.out.println("Arg2:"+tmp);
		tmp2 = tmp.split("\\.");
		String zone2 = tmp2[0];
		String name2 = tmp2[1];

		tmp = conf.get("TRANS.INPUT.OUTPUT.NAME");
		System.out.println("Output:"+tmp);
		tmp2 = tmp.split("\\.");
		String zone3 = tmp2[0];
		String name3 = tmp2[1];

		int[] start1 = UTILS.getCordinate(conf.get("TRANS.INPUT.ARG1.START"));
		int[] start2 = UTILS.getCordinate(conf.get("TRANS.INPUT.ARG2.START"));
		
		int[] off = UTILS.getCordinate(conf.get("TRANS.INPUT.ARG1.OFF"));
	//	int[] outPartition = UTILS.getCordinate(conf
	//			.get("TRANS.INPUT.OUTPUT.PARTITION"));

		int op = conf.getInt("TRANS.INPUT.OP",0);
		float m1 = conf.getFloat("TRANS.INPUT.ARG1.MODULUS", 1.0f);
		float m2 = conf.getFloat("TRANS.INPUT.ARG2.MODULUS", 1.0f);
		OptimusDoubleCalculator cal = null;
		switch (op) {
		case 1:
			cal = new OptimusDoubleCalculator('+');
			break;
		case 2:
			cal = new OptimusDoubleCalculator('-');
			break;
		case 3:
			cal = new OptimusDoubleCalculator('*');
			break;
		default:
			cal = new OptimusDoubleCalculator('/');
		}
		System.out.println("m1:"+m1);
		System.out.println("m2:"+m2);
		cal.setModulus1(m1*1.0);
		cal.setModulus2(m2*1.0);
		
		String confDir = conf.get("TRANS.CONF.DIR");
		if (confDir == null) {
			confDir = System.getenv("OPTIMUS_CONF");
		}
		List<InputSplit> splits = null;
		boolean earlyBird = conf.getBoolean("TRANS.EARLYBIRD", false);
		ZoneClient zclient = null;
		try {
			zclient = new ZoneClient(new OptimusConfiguration(confDir));
			OptimusCatalogProtocol ci = zclient.getCi();

			OptimusZone zone = zclient.openZone(zone1);
			OptimusArray array = ci.openArray(zone.getId(), new Text(name1));

			DataChunk chunk = new DataChunk(zone.getSize().getShape(), zone
					.getPstep().getShape());
			
			int zid1 = zone.getId().getId();
			int aid1 = array.getId().getArrayId();

			zone = zclient.openZone(zone2);
			array = ci.openArray(zone.getId(), new Text(name2));
			int zid2 = zone.getId().getId();
			int aid2 = array.getId().getArrayId();

			zone = zclient.openZone(zone3);
			array = ci.openArray(zone.getId(), new Text(name3));
			int zid3 = zone.getId().getId();
			int aid3 = array.getId().getArrayId();

			Set<DataChunk> chunks = chunk.getAdjacentChunks(start1, off);
			splits = new LinkedList<InputSplit>();
			for (DataChunk c : chunks) {
				TransBinaryInputSplit split = new TransBinaryInputSplit();
				
				split.setOff(c.getChunkSize());
				
				int[] cstart = c.getStart();
				int[] csize = c.getChunkSize();
				int[] nstart1 = this.getOverlapStart(start1, cstart);
				int[] nstart2 = new int [start1.length];
				int[] rstart = new int[start1.length];
				int[] roff = new int [start1.length];
				
				for (int i = 0; i < nstart1.length; i++) {
					rstart[i] = nstart1[i] - start1[i];
					roff[i] = cstart[i] + csize[i] - nstart1[i];
					
					nstart2[i] = nstart1[i] - start1[i] + start2[i];
				}
				//read start within result
				split.setRstart(rstart);
				//read range
				split.setRoff(roff);
				//start of the partition
				split.setCstart(cstart);
				//start of the first array
				split.setStart1(nstart1);
				//start in the second array
				split.setStart2(nstart2);

				
				//partition size
				split.setPs1(csize);
				//partition id
				split.setPnum1(c.getChunkNum());
				
				split.setConfDir(confDir);
				split.setCal(cal);
				split.setAid1(aid1);
				split.setAid2(aid2);
				split.setAid3(aid3);
				split.setEarlybird(earlyBird);
				split.setZid1(zid1);
				split.setZid2(zid2);
				split.setZid3(zid3);
				Partition pd = new Partition(new ZoneID(zid1),
						new ArrayID(aid1), new PID(c.getChunkNum()), new RID(0));
				TransHostList l = null;
				try {
					l = ci.getHosts(pd);
				} catch (WrongArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(-3);
				}
				split.setHosts(l);
				splits.add(split);
			}
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return splits;
	}

	private int[] getOverlapStart(int[] start, int[] newStart) {
		int[] ret = new int[start.length];

		for (int i = 0; i < ret.length; i++) {
			ret[i] = start[i] > newStart[i] ? start[i] : newStart[i];
		}
		return ret;
	}

	@Override
	public RecordReader<Object, TransBinaryMapInputValue> createRecordReader(
			InputSplit arg0, TaskAttemptContext context) throws IOException,
			InterruptedException {
		TransBinaryInputSplit split = (TransBinaryInputSplit) arg0;

		return new TransBinaryRecordReader();
	}

}
