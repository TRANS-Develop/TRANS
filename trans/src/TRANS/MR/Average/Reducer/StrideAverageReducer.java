package TRANS.MR.Average.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.jdom2.JDOMException;

import TRANS.Array.ArrayID;
import TRANS.Array.DataChunk;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Array.ZoneID;
import TRANS.Client.ZoneClient;
import TRANS.Data.TransDataType;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.MR.Average.StrideAverageResult;
import TRANS.MR.Median.StripeMedianResult;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.TRANSDataIterator;

public class StrideAverageReducer extends 	Reducer<IntWritable, StrideAverageResult, IntWritable, DoubleWritable> {
	
	private int []stride = null;
	private int []parshape = null;
	private int []rangeOff = null;
	private int []resultShape = null;
	private Double []result = null;
	private int cur = 0;
	
	private String confDir=null;
	boolean isFirst = true;
	private DataChunk localChunk = null;
	private int zid = 0;
	private int arrayid = 0;
	private int rnum = 0;
	private Counter co = null;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		co = (Counter)context.getCounter("TRANS_OUT", "REUCE_WRITE");
		JobConf conf = (JobConf)context.getConfiguration();
		String s = conf.get("TRANS.range.stride");

		String roff = conf.get("TRANS.range.offset");
		String pshape = conf.get("TRANS.output.array.pshape");
		this.arrayid = conf.getInt("TRANS.output.array.id", -1);
		this.zid = conf.getInt("TRANS.output.array.zoneid", -1);
		this.rnum = conf.getInt("TRANS.output.array.rnum", -1);
		if(this.arrayid < 0 || this.zid < 0 || this.rnum < 0)
		{
			throw new IOException("Wrong Array ID Get");
		}
		confDir = conf.get("TRANS.conf.dir");
	
		if(confDir==null)
		{
			confDir = System.getenv("OPTIMUS_CONF");
		}
		if(confDir == null)
		{
			confDir="/home/zhumeiqi/Replicate/conf";
		}
		String []st = s.split(",");
		String []pstr = pshape.split(",");
		String []offs = roff.split(",");
	
		
		stride = new int[st.length];
		parshape = new int[st.length];
		rangeOff = new int[st.length];
		
		resultShape = new int [st.length];
		
		for(int i = 0; i < st.length; i++)
		{
			stride[i] = Integer.parseInt(st[i]);
			parshape[i]=Integer.parseInt(pstr[i]);
			rangeOff[i]=Integer.parseInt(offs[i]);
			
			resultShape[i] = rangeOff[i]/stride[i];
		}	
			
		localChunk = new DataChunk(resultShape, parshape);
			
		super.setup(context);
	}

	public void reduce(IntWritable key, Iterable<StrideAverageResult> values,
			Context context) throws InterruptedException, IOException {
		if(this.isFirst)
		{
			this.isFirst = false;
			localChunk.getChunkByOff(key.get());
			int[] s = localChunk.getChunkSize();
			System.out.println(localChunk);
			System.out.println(Arrays.toString(s));
			int len = 1;
			for(int i = 0; i < s.length ; i++)
			{
				len *= s[i];
			}
			this.result = new Double[len];
		}
		Iterator<StrideAverageResult> it = values.iterator();
		StrideAverageResult result = it.next();
		if(result.isFull())
		{
			this.result[this.cur++] = result.getResult();
			//context.write(key, new DoubleWritable(result.getResult()));
		}else{
			while(it.hasNext())
			{
				result.addResult(it.next());
			}
			this.result[this.cur++] = result.getResult();
		}
	}

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		TRANSDataIterator itr = new TRANSDataIterator(new TransDataType(this.result.getClass()),this.result,localChunk.getStart(),localChunk.getChunkSize());
		co.increment(this.result.length * 8);
		Partition p = new Partition(new ZoneID(zid), new ArrayID(arrayid),
				new PID(localChunk.getChunkNum()), new RID(rnum - 1));
		ZoneClient client = null;
		try {
			client = new ZoneClient(new OptimusConfiguration(confDir));
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Host h ;
		try {
			BooleanWritable w = client.getCi().CreatePartition(p);
			h = client.getCi().getReplicateHost(p, p.getRid());
			
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException("Reading Replicate Host Error");
		}
		
		OptimusDataProtocol dp = h.getDataProtocol();
		System.out.println(Arrays.toString(itr.getData()));
		if(!dp.putPartitionData(p, itr).get())
		{
			throw new IOException("Ouput Result to TRANS FAILURE");
		}
		super.cleanup(context);
	}
}
