package TRANS.MR.Median.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jdom2.JDOMException;

import TRANS.Array.DataChunk;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusZone;
import TRANS.Client.ZoneClient;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.MR.Median.StrideResult;
import TRANS.MR.Median.StripeMedianResult;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.OptimusConfiguration;

/**
 * Mapper for the Average operator
 */
public class MedianMapper extends Mapper<IntWritable, StripeMedianResult, IntWritable, StripeMedianResult> {
  public static enum InvalidCell { INVALID_CELL_COUNT } ;

  Vector<Integer> fullIds = new Vector<Integer>();
  private Counter c = null;
  private Counter cf = null;
  private Counter cnf = null;
  @Override
protected void setup(Context context) throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	c = (Counter)context.getCounter("TRANS_READ","MAPPER_READ");
	cf = (Counter)context.getCounter("TRANS_READ","MAPPER_READ_FULL_RECORD");
	cnf = (Counter)context.getCounter("TRANS_READ","MAPPER_READ_NOT_FULL");
	super.setup(context);
}
DataChunk chunk = null;
/**
   * Reduces values for a given key
   * @param key the Key for the given value being passed in
   * @param value an Array to process that corresponds to the given key 
   * @param context the Context object for the currently executing job
   */
  public void map(IntWritable key, StripeMedianResult value, Context context)
                  throws IOException, InterruptedException {
	  c.increment(value.getSize());	  
	  if(value.isFull())
	  {
		  cf.increment(1);
	  }else{
		  cnf.increment(1);
	  }
	  context.write(key, value);
   }
      
}
