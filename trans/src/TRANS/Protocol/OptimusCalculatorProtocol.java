package TRANS.Protocol;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

import TRANS.Array.ArrayID;
import TRANS.Array.OptimusShape;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Calculator.OptimusCalculator;
import TRANS.Data.Optimus1Ddata;
import TRANS.util.OptimusData;

public interface OptimusCalculatorProtocol extends VersionedProtocol {
	static public long versionID = 1; 
	public OptimusData FindMaxMin(ArrayID aid, PID pid,OptimusShape pshape, OptimusShape start, OptimusShape off)throws Exception;
	public OptimusData JoinArray(Partition array1, Partition array2, 
			OptimusShape pshape, OptimusShape rstart, OptimusShape roff,
			OptimusCalculator c)throws IOException;
	 
}
