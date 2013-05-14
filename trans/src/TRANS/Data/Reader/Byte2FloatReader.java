package TRANS.Data.Reader;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Byte2FloatReader implements ByteReader{
	static int elesize = 4;
	public int size;
	byte []data;
	int cur;
	DataInputStream  datain = null;
	RandomAccessFile rin = null;
	DataOutputStream out = null;
	public Byte2FloatReader(){};
	public Byte2FloatReader(int size, DataOutputStream out,DataInputStream in)
	{
		this.size = size;
		data = new byte [this.size * elesize];
		this.datain = in;
		this.out = out;
	}
	
	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
		this.cur = this.data.length;
	}

	public Byte2FloatReader(int size, DataOutputStream out,RandomAccessFile in)
	{
		this.size = size;
		data = new byte [this.size * elesize];
		this.rin = in;
		this.out = out;
	}
	
	public void readFromin() throws IOException
	{
		int r = -1;
		if( rin != null)
		{
			r = rin.read(data, this.cur, (this.size*elesize - this.cur));
			
		}else{
			r = datain.read(data, this.cur, (this.size*elesize - this.cur));
		
		}
		if(out != null && r != -1)
		{
			//System.out.println((this.size * 8)+" ==? "+this.cur+"+"+r);
			out.write(data,this.cur,r);
		}
		if( r > 0){
			this.cur += r;
		}
	}
	
	public void readFromin(int len) throws IOException
	{
		int r;
		int rlen = (this.size*elesize - this.cur) > len * elesize ? len * elesize : (this.size*elesize - this.cur);
		if( rin != null)
		{
			r = rin.read(data, this.cur, rlen );
		}else{
			r = datain.read(data, this.cur, rlen );
			
		}
		if(out != null)
		{
			out.write(data,this.cur,r);
		}
		this.cur += r;
	}
	
	public Object [] readData()
	{
		Float [] fdata = new Float[this.cur/elesize];
		int l ; 
		int index = 0;
		for(int i = 0 ; i < fdata.length; i++)
		{ 
			l = data[index ++];  
	        l &= 0xff;  
	        l |= ((long) data[index ++] << 8);  
	        l &= 0xffff;  
	        l |= ((long) data[index ++] << 16);  
	        l &= 0xffffff;  
	        l |= ((long) data[index ++] << 24);  
	        
	        fdata[i] = Float.intBitsToFloat(l); 
		}
		for(int i = 0; i < this.cur %elesize ; i++)
		{
			this.data[i] = this.data[index++];
		}
		this.cur %=elesize;
		return fdata;
	}
		/*
	public  Object[] readData(){
		if( this.cur < 8)
		{
			return null;
		}
		Double [] ddata = new Double[this.cur/8];
		long l;
		int index = 0;
		for(int i = 0 ; i < ddata.length; i++)
		{ 
			l=data[index++];
			l&=0xff;
			l|=((long)data[index++]<<8);
			l&=0xffff;
			l|=((long)data[index++]<<16);
			l&=0xffffff;
			l|=((long)data[index++]<<24);
			l&=0xffffffffl;
	    	l|=((long)data[index++]<<32);
	    	l&=0xffffffffffl;

	    	l|=((long)data[index++]<<40);
	    	l&=0xffffffffffffl;
	    	l|=((long)data[index++]<<48);
	    	l&=0xffffffffffffffl;
	    	l|=((long)data[index++]<<56);
	    	ddata [i] = Double.longBitsToDouble(l);
		}
		int start = this.cur - this.cur%8;
		for( int i = 0; i < this.cur%8; i++)
		{
			this.data[i] = this.data[start+i];
		}
		this.cur %= 8;
		return (Object [])ddata;
	}
	*/
}
