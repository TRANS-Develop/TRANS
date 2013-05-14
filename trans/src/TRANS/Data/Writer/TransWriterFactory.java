package TRANS.Data.Writer;

import java.io.DataOutput;
import java.io.RandomAccessFile;

import TRANS.Array.Partition;
import TRANS.Data.Writer.Interface.ByteWriter;

public class TransWriterFactory {
	public static ByteWriter getStreamWriter(Class<?>type,int size, DataOutput out)
	{
		if(type.equals(Double.class))
		{
			return new OptimusDouble2ByteStreamWriter(size,out);
		}else if(type.equals(Float.class))
		{
			return new TransFloat2ByteStreamWriter(size,out);
		}else{
			System.out.println("UnSupported DataType");
			return null;
		}
	}
	public static ByteWriter getRandomWriter(Class<?>type,int size, RandomAccessFile out,Partition p)
	{
		if(type.equals(Double.class))
		{
			return new OptimusDouble2ByteRandomWriter(size,out,p);
		}else if(type.equals(Float.class))
		{
			return new TransFloat2ByteRandomWriter(size,out,p);
		}else{
			System.out.println("UnSupported DataType");
			return null;
		}
	}
}
