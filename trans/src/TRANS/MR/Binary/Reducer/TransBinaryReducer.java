package TRANS.MR.Binary.Reducer;

import java.io.IOException;

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
import TRANS.Exceptions.WrongArgumentException;
import TRANS.MR.Median.StripeMedianResult;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.TRANSDataIterator;

public class TransBinaryReducer	extends Reducer<IntWritable, TRANSDataIterator, IntWritable, DoubleWritable>{
	
	OptimusCatalogProtocol ci = null;
	private int zid = 0;
	private int aid = 0;
	private int rnum = 0;
	private Counter co = null;
	private Counter cf = null;
	private Counter cnf = null;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		JobConf conf = (JobConf)context.getConfiguration();
		String confDir = conf.get("TRANS.conf.dir");
		if (confDir == null) {
			confDir = System.getenv("OPTIMUS_CONF");
		}
		
		zid = conf.getInt("TRANS.OUTPUT.ZID", -1);
		aid = conf.getInt("TRANS.OUTPUT.AID", -1);
		rnum = conf.getInt("TRANS.OUTPUT.RNUM", -1);
		if(zid == -1 || aid == -1 || rnum == -1)
		{
			throw new IOException("Configuration Error,Job Not Configured Properly");
		}
		ZoneClient zclient = null;
		try {
			 zclient = new ZoneClient(new OptimusConfiguration(confDir));
			 this.ci = zclient.getCi();
		} catch (WrongArgumentException e) {
			throw new IOException("Configuration Error,Job Not Configured Properly");
		} catch (JDOMException e) {
			throw new IOException("Configuration Error,Job Not Configured Properly");
		}
		co = (Counter)context.getCounter("TRANS_WRITE", "REDUCE_WRITE");
			
		super.setup(context);
	}
	public void reduce(IntWritable key, Iterable<TRANSDataIterator> values,
			Context context) throws InterruptedException, IOException {
		Partition p = new Partition(new ZoneID(zid),new ArrayID(aid),new PID(key.get()),new RID(rnum -1));
		
		Host h;
		try {
			h = ci.getReplicateHost(p, new RID(rnum -1));
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException("Get Replicate Host Error");
		}
		OptimusDataProtocol dp = h.getDataProtocol();
		for(TRANSDataIterator itr:values)
		{
			dp.putPartitionData(p, itr);
			co.increment(32+itr.getData().length * 8);
		}
	}
	
}
