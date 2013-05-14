package TRANS.Protocol;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

import TRANS.Array.ArrayID;
import TRANS.Array.OptimusShape;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Data.OptimusData;
import TRANS.MR.Average.AverageResultArrayWritable;
import TRANS.MR.Median.MedianResultArrayWritable;
import TRANS.MR.io.AverageResult;
import TRANS.util.Host;
import TRANS.util.TRANSDataIterator;

public interface OptimusDataProtocol extends VersionedProtocol {
	/**
	 * @param h: ��Ҫ���ݵ��ĵط�
	 * @param shape�� Ŀ�������chunk��ʽ
	 * @return�� �Ƿ�ɹ�
	 */
	static public long versionID = 1; 
	public IntWritable RecoverReadAll(Host h,OptimusShape psize,Partition p,RID rid);
	public OptimusData readData(ArrayID aid,PID pid, OptimusShape pshape, OptimusShape start, OptimusShape off) throws IOException;
	public AverageResult readAverage(ArrayID aid,PID pid,OptimusShape pshape, OptimusShape start, OptimusShape off) throws IOException;
	public IntWritable RecoverPartition(Partition p,OptimusShape psize,Host host);
	/*p the identify of partition
	 *data 
	 *rnum the identify of the replica
	 * */
	public BooleanWritable putPartitionData(Partition p, TRANSDataIterator data) throws IOException;
	public MedianResultArrayWritable  readStride(Partition p, OptimusShape pshape,OptimusShape start, 
			OptimusShape off,OptimusShape stride)throws IOException;
	
	public AverageResultArrayWritable readStrideAverage(Partition p,OptimusShape pshape, OptimusShape start,
			OptimusShape off,OptimusShape stride)throws IOException;
	 
}
