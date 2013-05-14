package TRANS.Data.Reader;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class TransByteReaderFactory {
	public static ByteReader getByteReader(Class<?>type,int size, DataOutputStream out,DataInputStream in)throws IOException
	{
		if(type.equals(Double.class))
		{
			return new Byte2DoubleReader(size,out,in);
		}else if(type.equals(Float.class))
		{
			return new Byte2FloatReader(size,out,in);
		}else {
			System.out.println();
			throw new IOException("UnSupported type");
		}
	}

	public static ByteReader getByteReader(Class<?>type,int size, DataOutputStream out,RandomAccessFile in)throws IOException
	{
		if(type.equals(Double.class))
		{
			return new Byte2DoubleReader(size,out,in);
		}else if(type.equals(Float.class))
		{
			return new Byte2FloatReader(size,out,in);
		}else {
			System.out.println();
			throw new IOException("UnSupported type");
		}
	}
}
