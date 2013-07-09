package TRANS.MR.Average.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import TRANS.Data.Optimus1Ddata;
import TRANS.Data.OptimusData;
import TRANS.Data.TransDataType;
import TRANS.MR.io.AverageResult;

/**
 * Mapper for the Average operator
 */
public class AverageMapper extends Mapper<Object, OptimusData, LongWritable, AverageResult> {
	
  public static enum InvalidCell { INVALID_CELL_COUNT } ;
  private Counter c = null;
  private TransDataType type = null;
  @Override
protected void setup(Context context) throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	  c = (Counter) context.getCounter("TRANS_READ", "MAPPER_READ");
	  JobConf conf = (JobConf)context.getConfiguration();
	  String typeString = conf.get("TRANS.array.type","NOT_DEFINED");
	  this.type = TransDataType.getTypeFromString(typeString);
	  super.setup(context);
}

/**
   * Reduces values for a given key
   * @param key the Key for the given value being passed in
   * @param value an Array to process that corresponds to the given key 
   * @param context the Context object for the currently executing job
   */
  public void map(Object key, OptimusData value, Context context)
                  throws IOException, InterruptedException {
	  
    AverageResult r = new AverageResult();
    r.setType(this.type);
   	Object []data = value.getData();
    c.increment(data.length);
    r.addAll(data);
    context.write(new LongWritable(1), r);
   }
      
}
