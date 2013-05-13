package TRANS.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import TRANS.Array.OptimusShape;

public class TRANSDataIterator implements Writable{
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(shape);
		result = prime * result + Arrays.hashCode(start);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TRANSDataIterator other = (TRANSDataIterator) obj;
		if (!Arrays.equals(shape, other.shape))
			return false;
		if (!Arrays.equals(start, other.start))
			return false;
		return true;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int[] getStart() {
		return start;
	}

	public void setStart(int[] start) {
		this.start = start;
	}

	public int[] getShape() {
		return shape;
	}

	public void setShape(int[] shape) {
		this.shape = shape;
	}

	//data to read
	double []data = null;
	//the description of the data
	int [] start = null;
	@Override
	public String toString() {
		return "TRANSDataIterator [data=" + Arrays.toString(data) + ", start="
				+ Arrays.toString(start) + ", shape=" + Arrays.toString(shape)
				+ ", rstart=" + Arrays.toString(rstart) + ", roff="
				+ Arrays.toString(roff) + ", size=" + size + ", volume="
				+ volume + "]";
	}

	int [] shape = null;
	//the description of read operation
	int [] rstart = null;
	int [] roff = null;

	int size = 0;
	int volume = 0;
	int[] fjump = null;
	int fpos = 0;
	private int[] itr;
	public TRANSDataIterator(){}
	
	public TRANSDataIterator(double []data, int []start, int []shape)
	{
		this.data = data;
		this.shape = shape;
		this.start = start;		
		volume = 1;
		for(int i = 0 ; i < shape.length; i++)
			volume *= shape[i];
	}
	public boolean init(int[] s, int[] o)
	{
		int len = start.length;
		this.rstart = new int[len];
		this.roff = new int[len];
		for(int i = 0 ; i < len; i++)
		{
			this.rstart[i] = Math.max(s[i],start[i]);
			this.roff[i] = Math.min(s[i]+o[i], start[i]+shape[i]);
			this.roff[i] -= this.rstart[i];
			if(this.roff[i] <= 0)
				return false;
		}
		
		this.fjump = new int[start.length];
		fpos = 0;
		for (int i = 0; i < start.length ; i++) {
			fpos = fpos * shape[i] + rstart[i] - start[i];
		}
		fjump[start.length - 1] = shape[start.length - 1];
		for (int i = start.length - 2; i >= 0; i--) {
			fjump[i] = shape[i] * fjump[i + 1];
		}
		len = start.length - 1;
		itr = new int[len + 1];
		itr[len]=-1;
		return true;
	}
	public boolean next(){
		int len = start.length - 1;
		itr[len]++;
		if(itr[len] >= roff[len])
		{
			int j = len - 1;
			while (j >= 0) {
				itr[j]++;
				fpos += fjump[j + 1];
				if (itr[j] < roff[j]) {
					break;
				} else if (j == 0) {
					break;
				} else {
					fpos -= itr[j] * fjump[j + 1];
					itr[j] = 0;
				}
				j--;
			}
			if(itr[0] >= roff[0]) return false;
			itr[len]=0;
		}
		return true;
	}
	public double get()
	{
		return this.data[fpos+itr[itr.length - 1]];
	}
	public void set(double d)
	{
		this.data[fpos+itr[itr.length - 1]] = d;
	}
	public void add(double d)
	{
		this.size++;
		this.data[fpos+itr[itr.length - 1]] = d;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
	
		new OptimusShape(this.start).write(out);
		new OptimusShape(this.shape).write(out);
		OptimusDouble2ByteStreamWriter writer = 
				new OptimusDouble2ByteStreamWriter(this.data.length * 8,out);
		writer.writeDouble(this.data);
		writer.close();
		WritableUtils.writeVInt(out, this.size);
		WritableUtils.writeVInt(out, this.volume);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		
		OptimusShape s = new OptimusShape();
		s.readFields(in);
		this.start = s.getShape();
		s.readFields(in);
		this.shape = s.getShape();
		
		Byte2DoubleReader reader = new Byte2DoubleReader();
		
		
		
		int len = 1, l = shape.length;
		for( int i = 0; i < l; i++)
		{
			len *= shape[i];
		}
		byte[] bdata = new byte[len*8];  
		in.readFully(bdata);
		reader.setData(bdata);
		this.data = reader.readData();
		this.size = WritableUtils.readVInt(in);
		this.volume = WritableUtils.readVInt(in);
	}
	
	public static void main(String []args)
	{
		double []data = new double[9*5*4];
		for(int i=0; i < data.length;i++)
		{
			data[i]=i+1;
		}
		int []start={0,0,0};
		int []shape={9,5,4};
		int [] rstart={0,0,0};
		int [] roff={9,5,4};
		double []rdata = new double[9*5*4];
		TRANSDataIterator ritr = new TRANSDataIterator(data,start,shape);
		TRANSDataIterator citr = new TRANSDataIterator(rdata,rstart,roff);
		
		ritr.init(rstart, roff);
		citr.init(start, shape);
		while(ritr.next())
		{
			citr.next();
			System.out.println(ritr.get());
			citr.set(ritr.get());
			
		}
		
	}

	public double[] getData() {
		return data;
	}

	public void setData(double[] data) {
		this.data = data;
	}
	public boolean isFull()
	{
		return this.size >= this.volume;
	}
	public void setFull()
	{
		this.size = this.volume;
	}
}
