package TRANS.MR.Binary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import TRANS.Array.OptimusShape;
import TRANS.Calculator.OptimusCalculator;
import TRANS.Calculator.OptimusDoubleCalculator;
import TRANS.util.Host;
import TRANS.util.TransHostList;

public class TransBinaryInputSplit extends InputSplit implements Writable{
	public OptimusCalculator getCal() {
		return cal;
	}

	public void setCal(OptimusCalculator cal) {
		this.cal = cal;
	}
	OptimusCalculator cal = null;
	public int[] getStart1() {
		return start1;
	}

	public void setStart1(int[] start1) {
		this.start1 = start1;
	}

	public int[] getStart2() {
		return start2;
	}

	public void setStart2(int[] start2) {
		this.start2 = start2;
	}

	public int[] getOff() {
		return off;
	}

	public void setOff(int[] off) {
		this.off = off;
	}
	private int pnum1 = 0;
	public int getPnum1() {
		return pnum1;
	}

	public void setPnum1(int pnum1) {
		this.pnum1 = pnum1;
	}
	private int zid1=0;
	private int aid1=0;
	public int getZid1() {
		return zid1;
	}

	public void setZid1(int zid1) {
		this.zid1 = zid1;
	}

	public int getAid1() {
		return aid1;
	}

	public void setAid1(int aid1) {
		this.aid1 = aid1;
	}

	public int getZid2() {
		return zid2;
	}

	public void setZid2(int zid2) {
		this.zid2 = zid2;
	}

	public int getAid2() {
		return aid2;
	}

	public void setAid2(int aid2) {
		this.aid2 = aid2;
	}

	public int getZid3() {
		return zid3;
	}

	public void setZid3(int zid3) {
		this.zid3 = zid3;
	}

	public int getAid3() {
		return aid3;
	}

	public void setAid3(int aid3) {
		this.aid3 = aid3;
	}
	private int zid2=0;
	private int aid2=0;
	private int zid3=0;
	private int aid3=0;	
	//partition size
	private int []ps1 = null;
	public int[] getPs1() {
		return ps1;
	}

	public void setPs1(int[] ps1) {
		this.ps1 = ps1;
	}
	
	//start in the orignal array
	private int []start1 = null;
	private int []start2 = null;
	private int []off = null;
	//start of the partition
	private int []cstart = null;
	public int[] getCstart() {
		return cstart;
	}

	public void setCstart(int[] cstart) {
		this.cstart = cstart;
	}
	//start of in the result
	private int []rstart = null;
	private int []roff = null;
	private String confDir = null;
	private TransHostList hosts = new TransHostList();
	boolean earlybird = false;
	

	public TransHostList getHosts() {
		return hosts;
	}

	public void setHosts(TransHostList hosts) {
		this.hosts = hosts;
	}

	public String getConfDir() {
		return confDir;
	}

	public void setConfDir(String confDir) {
		this.confDir = confDir;
	}

	public int[] getRstart() {
		return rstart;
	}

	public void setRstart(int[] rstart) {
		this.rstart = rstart;
	}

	public int[] getRoff() {
		return roff;
	}

	public void setRoff(int[] roff) {
		this.roff = roff;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeVInt(out, aid1);
		WritableUtils.writeVInt(out, aid2);
		WritableUtils.writeVInt(out, aid3);
		WritableUtils.writeVInt(out, zid1);
		WritableUtils.writeVInt(out, zid2);
		WritableUtils.writeVInt(out, zid3);
		WritableUtils.writeVInt(out, this.pnum1);
		WritableUtils.writeString(out, this.confDir);
		new OptimusShape(this.start1).write(out);
		new OptimusShape(this.start2).write(out);
		new OptimusShape(this.off).write(out);
		new OptimusShape(this.rstart).write(out);
		new OptimusShape(this.off).write(out);
		new OptimusShape(this.ps1).write(out);
		this.hosts.write(out);
		this.cal.write(out);
		
		new BooleanWritable(this.earlybird).write(out);
		
		new OptimusShape(this.cstart).write(out);
		
	}

	@Override
	public String toString() {
		return "TransBinaryInputSplit [cal=" + cal + ", pnum1=" + pnum1
				+ ", zid1=" + zid1 + ", aid1=" + aid1 + ", zid2=" + zid2
				+ ", aid2=" + aid2 + ", zid3=" + zid3 + ", aid3=" + aid3
				+ ", ps1=" + Arrays.toString(ps1) + ", start1="
				+ Arrays.toString(start1) + ", start2="
				+ Arrays.toString(start2) + ", off=" + Arrays.toString(off)
				+ ", rstart=" + Arrays.toString(rstart) + ", confDir="
				+ confDir + ", hosts=" + hosts + "]";
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.aid1 = WritableUtils.readVInt(in);
		this.aid2 = WritableUtils.readVInt(in);
		this.aid3 = WritableUtils.readVInt(in);
	
		this.zid1 = WritableUtils.readVInt(in);
		this.zid2 = WritableUtils.readVInt(in);
		this.zid3 = WritableUtils.readVInt(in);
		
		this.pnum1 = WritableUtils.readVInt(in);
		
		this.confDir = WritableUtils.readString(in);
		
		OptimusShape tmp = new OptimusShape();
		tmp.readFields(in);
		this.start1 = tmp.getShape();
		
		tmp.readFields(in);
		this.start2 = tmp.getShape();
		
		tmp.readFields(in);
		this.off = tmp.getShape();
		
		tmp.readFields(in);
		this.rstart = tmp.getShape();
		tmp.readFields(in);
		this.roff = tmp.getShape();
		
		tmp.readFields(in);
		this.ps1 = tmp.getShape();
		this.hosts = new TransHostList();
		this.hosts.readFields(in);
		this.cal = new OptimusDoubleCalculator();
		this.cal.readFields(in);
		
		BooleanWritable tb = new BooleanWritable();
		tb.readFields(in);
		this.earlybird = tb.get();
		tmp.readFields(in);
		this.cstart = tmp.getShape();
	}

	public boolean isEarlybird() {
		return earlybird;
	}

	public void setEarlybird(boolean earlybird) {
		this.earlybird = earlybird;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		
		Vector<Host> hosts = this.hosts.getHosts();
		String []ret = new String[hosts.size()];
		for(int i = 0; i < hosts.size();i++)
		{
			ret[i] = hosts.get(i).getHost();
		}
		System.out.println("Hosts:"+Arrays.toString(ret));
		return ret;
	}
	
}
