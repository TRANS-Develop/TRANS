package TRANS.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import TRANS.Array.OptimusShape;
import TRANS.Exceptions.WrongArgumentException;


/**
 * @author foryee
 *
 */
public class OptimusData implements Writable {
	
	public TransDataType getType() {
		return type;
	}
	public void setType(TransDataType type) {
		this.type = type;
	}

	private TransDataType type = new TransDataType();
	private OptimusShape start;
	private OptimusShape off;
	private OptimusShape shape;
	Object [] data = null;
	
	public OptimusData(){}
	public OptimusData(Object []data,OptimusShape start, OptimusShape off,OptimusShape shape) throws IOException
	{
		this.data = data;
		this.start = start;
		this.off = off;
		this.shape = shape;
		this.type = new TransDataType(data[0].getClass());
	}
	public OptimusShape getShape() {
		return shape;
	}
	public void setShape(OptimusShape shape) {
		this.shape = shape;
	}
	public OptimusShape getStart() {
		return start;
	}
	public void setStart(OptimusShape start) {
		this.start = start;
	}
	public OptimusShape getOff() {
		return off;
	}
	public void setOff(OptimusShape off) {
		this.off = off;
	}
	
	private void writeDouble(DataOutput out) throws IOException 
	{
		//Double []ddata = (Double [])this.data;
		for(int i = 0 ; i < this.data.length; i++)
		{
			//out.writeFloat(this.data[i]);
			out.writeDouble((Double)data[i]);
		}
	}
	private void readDouble(DataInput in)throws IOException
	{
		for(int i = 0 ; i < this.data.length; i++)
		{
			this.data[i] = in.readDouble();
		}
	}
	private void writeFloat(DataOutput out) throws IOException 
	{
		//Float []ddata = (Float [])this.data;
		for(int i = 0 ; i < this.data.length; i++)
		{
			out.writeFloat((Float)data[i]);
		}
	}
	private void readFloat(DataInput in)throws IOException
	{
		for(int i = 0 ; i < this.data.length; i++)
		{
			this.data[i] = in.readFloat();
		}
	}
	private void writeInteger(DataOutput out) throws IOException 
	{
		//Integer []ddata = (Integer [])this.data;
		for(int i = 0 ; i < this.data.length; i++)
		{
			out.writeInt((Integer)data[i]);
		}
	}
	private void readInteger(DataInput in)throws IOException
	{
		for(int i = 0 ; i < this.data.length; i++)
		{
			this.data[i] = in.readInt();
		}
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		this.type.write(out);
		if(this.data == null)
		{
			out.writeInt(0);
			return;
		}
		out.writeInt(this.data.length);
		Class<?> dc = TransDataType.getClass(type);
		if(dc.equals(Double.class))
		{
			this.writeDouble(out);
		}else if(dc.equals(Float.class))
		{
			this.writeFloat(out);
		}else if(dc.equals(Integer.class))
		{
			this.writeInteger(out);
		}else{
			System.out.println("Unknown Data Type");
			throw new IOException("Unknown Data Type");
		}
	/*	for(int i = 0 ; i < this.data.length; i++)
		{
			//out.writeFloat(this.data[i]);
			out.writeDouble(this.data[i]);
		}
	*/
			
	}
	public Object[] getData() {
		return data;
	}
	public void setData(Object[] data) {
		this.data = data;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.type.readFields(in);
		int l = in.readInt();
		this.data = new Object[l];
		Class<?> dc = TransDataType.getClass(type);
		if(dc.equals(Double.class))
		{
			this.readDouble(in);
		}else if(dc.equals(Float.class))
		{
			this.readFloat(in);
		}else if(dc.equals(Integer.class))
		{
			this.readInteger(in);
		}else{
			throw new IOException("Unknown Data Type");
		}
	}
	
	/**
	 * @param off
	 * @param inPartiton: off ��partition�ڲ���offset ����ȫ�ֵ�offset
	 * @return
	 * @throws WrongArgumentException 
	 */
	public Object getData(OptimusShape off,boolean inPartition) throws WrongArgumentException
	{
		int []roff = new int [this.off.getShape().length];
		int [] toff = off.getShape();
		int [] start = off.getShape();
		int [] size = this.shape.getShape();
		if( !inPartition )
		{
			for( int i = 0 ; i < roff.length; i++ )
			{
				roff[i] = toff[i] - start[i];
				if( roff[i] < 0 )
				{
					throw new WrongArgumentException("off","rang range to read");
				}
			}
		}else{
			for( int i = 0 ; i < roff.length; i++ )
			{
				roff[i] = toff[i];
			}
		}
		int offset = roff[roff.length - 1];
		for( int  i = roff.length - 2; i>=0; i--)
		{
			offset = offset * size[ i +1 ] + roff[i];
		}
		if(offset >= this.data.length)
		{
			throw new WrongArgumentException("off","rang range to read");
		}
		return this.data[offset];
	}

	public Object getDataByOff(int off) throws WrongArgumentException
	{
		if(off >= this.data.length)
		{
			throw new WrongArgumentException("off","rang range to read");
		}
		return this.data[off];
	}

}
