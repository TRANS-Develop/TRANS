package TRANS.Data.Writer;

import java.io.IOException;
import java.io.RandomAccessFile;

import TRANS.Array.Partition;
import TRANS.Data.Writer.Interface.ByteWriter;


public class OptimusDouble2ByteRandomWriter implements ByteWriter {

	public int size;
	byte[] data;
	int cur;
	private Partition p = null;
	RandomAccessFile rout = null;

	public OptimusDouble2ByteRandomWriter(int size, RandomAccessFile out,Partition p) {
		this.size = size;
		data = new byte[this.size * 8];
		this.rout = out;
		this.p = p;
	}
	public void write(Object w) throws IOException
	{
		this.writeDouble((Double)w);
	}
	public void write(Object []w) throws IOException
	{
		this.writeDouble((Double [])w);
	}

	private void writeDouble(Double f) throws IOException {
		if (cur >= size * 8) {

			this.rout.write(data);
			this.cur = 0;
		}

		long l = Double.doubleToLongBits(f);

		for (int i = 0; i < 8; i++) {
			data[cur++] = new Long(l).byteValue();// new Integer(l).byteValue();
			l = l >> 8;
		}
	}

	private void writeDouble(Double[] fs) throws IOException {
		if (this.size < fs.length) {
			this.rout.write(data, 0, this.cur);
			this.data = new byte[fs.length * 8];
		}
		int i;
		for (int j = 0; j < fs.length; j++) {

			long l = Double.doubleToLongBits(fs[j]);
			for (i = 0; i < 8; i++) {
				data[cur++] = new Long(l).byteValue();// new
														// Integer(l).byteValue();
				l = l >> 8;
			}
			/*
			 * l = Float.floatToIntBits(fs[j]); for (i = 0; i < 8; i++) {
			 * data[cur++] = new Integer(l).byteValue(); l = l >> 8; }
			 */
		}

	}

	public void close() throws IOException {

		this.rout.write(data, 0, this.cur);
		//this.rout.close();
		p.close();
		//this.rout = null;

	}

}
