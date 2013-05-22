package TRANS.MR.Median;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.WritableComparable;

import TRANS.Array.OptimusShape;
import TRANS.Data.TransDataType;
import TRANS.Data.Reader.Byte2DoubleReader;
import TRANS.Data.Writer.OptimusDouble2ByteStreamWriter;
import TRANS.Data.Writer.Interface.ByteWriter;
import TRANS.MR.OptimusResultKey;

public class StripeMedianResult<T extends Comparable<T>> implements Writable {
	public int[] getStart() {
		return start;
	}

	public void setStart(int[] start) {
		this.start = start;
	}

	public int[] getStride() {
		return stride;
	}

	public void setStride(int[] stride) {
		this.stride = stride;
	}
	private int id=0;
	
	Vector<T> data = new Vector<T>();
	Set<OptimusResultKey> keys = new HashSet<OptimusResultKey>();
	private TransDataType type = new TransDataType();
	private int volume = 1;
	private T result = null;
	private boolean full = false;
	private int []start = null;
	private int []stride = null;
	public boolean isOverlaped( int []start, int []off)
	{
		boolean isIntersect = true;
		for (int i = 0; i < stride.length; i++) {
			if ((this.start[i] >= start[i] + off[i] || this.start[i]
					+ this.stride[i] <= start[i])) {
				isIntersect = false;
			}
		}
		return isIntersect;
	}
	public StripeMedianResult(){}
	public StripeMedianResult(int id,int []start, int []stride,Class<?> type) throws IOException
	{
		this.id = id;
		for(int i=0; i < stride.length; i++)
		{
			volume *= stride[i];
		}
		this.start = start;
		this.stride = stride;
		this.type = new TransDataType(type);
	}
	public boolean isFull()
	{
		if(this.full == true){
			return true;
		}else{
			return (this.data.size() >= this.volume);
		}
	}
	public boolean getFull()
	{
		return this.full;
	}
	public boolean contains(OptimusResultKey key)
	{
		return keys.contains(key);
	}
	public void add(T d)
	{
		data.add(d);
	}
	public void add(T []d)
	{
		for(int i=0;i<d.length;i++)
			data.add(d[i]);
	}
	public void add(StripeMedianResult<T> r)
	{
		Vector<T> data = (Vector<T>)r.getData();
		if(data != null)
			this.data.addAll(data);
	}
	public Vector<T> getData() {
		return data;
	}
	public void setData(Vector<T> data) {
		this.data = data;
	}
	public Set<OptimusResultKey> getKeys() {
		return keys;
	}
	public void setKeys(Set<OptimusResultKey> keys) {
		this.keys = keys;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.type.write(out);
		WritableUtils.writeVInt(out, this.id);
	
		new OptimusShape(this.start).write(out);
		this.full = this.isFull();
		new BooleanWritable(full).write(out);
		if(this.full)
		{
			T tmp = this.getResult();
			Class<?>t = TransDataType.getClass(this.type);
			if(t.equals(Double.class))
			{
				new DoubleWritable((Double)tmp).write(out);
			}else if(t.equals(Float.class))
			{
				new FloatWritable((Float)tmp).write(out);
			}else if(t.equals(Integer.class))
			{
				new IntWritable((Integer)tmp).write(out);
			}else{
				System.out.println("Unsupported type@median result write");
				throw new IOException("Unsupported type@median result write");
			}
			
		}else{
			WritableUtils.writeVInt(out, volume);
			
			int len = this.data.size();
			WritableUtils.writeVInt(out, len);
			//TODO
			ByteWriter writer = 
					new OptimusDouble2ByteStreamWriter(this.data.size() * 8,out);
			
			for(Object d: this.data)
				writer.write(d);
			if(len == 0)
			{
				System.out.println(this);
			}
			writer.close();
		}
	}
	public long getSize()
	{
		int ret = this.start.length * 4;
		if(this.isFull())
		{
			ret += 8;
		}else{
			ret += 8*this.data.size();
		}
		return ret;
	}
	@Override
	public String toString() {
		return "StripeMedianResult [id=" + id + ", data=" + data + ", keys="
				+ keys + ", volume=" + volume + ", result=" + result
				+ ", full=" + full + ", start=" + Arrays.toString(start)
				+ ", stride=" + Arrays.toString(stride) + "]";
	}

	public T getResult()
	{
		if(this.isFull()&&this.data.size() == 0)
		{
			return (T)this.result;
		}else{
			System.out.println(Arrays.toString(this.data.toArray()));
			Collections.sort(this.data);
			this.result = this.data.get(this.data.size()/2);
			this.data = null;
		}
		return (T)this.result;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.type.readFields(in);
		this.id = WritableUtils.readVInt(in);
		OptimusShape s = new OptimusShape();
		s.readFields(in);
		this.start = s.getShape();
		
		BooleanWritable b = new BooleanWritable();
		b.readFields(in);
		this.full =b.get();
		if(this.full)
		{
			Class<?>t = TransDataType.getClass(this.type);
			if(t.equals(Double.class))
			{
				DoubleWritable d = new DoubleWritable();
				d.readFields(in);
				this.result = (T)new Double(d.get());
			}else if(t.equals(Float.class))
			{
				FloatWritable d = new FloatWritable();
				d.readFields(in);
				this.result = (T) new Float(d.get());
			}else if(t.equals(Integer.class))
			{
				IntWritable d = new IntWritable();
				d.readFields(in);
				this.result = (T) new Integer( d.get());
			}else{
				System.out.println("Unsupported type@median result reaad");
				throw new IOException("Unsupported type@median result reaad");
			}
			
		}else{
		//	System.out.println(this.volume+":" + len);
			this.volume = WritableUtils.readVInt(in);
			int len = WritableUtils.readVInt(in);
			
			Byte2DoubleReader reader = new Byte2DoubleReader();
			byte[] bdata = new byte[len*8];  
			in.readFully(bdata);
			reader.setData(bdata);
			this.data = new Vector<T>();
			if(len != 0)
				this.add((T[])reader.readData());
		}
		
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
}
