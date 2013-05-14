package TRANS.Calculator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;

public class OptimusDoubleCalculator extends OptimusCalculator {

	public double getModulus1() {
		return modulus1;
	}
	public void setModulus1(double modulus1) {
		this.modulus1 = modulus1;
	}
	public double getModulus2() {
		return modulus2;
	}
	public void setModulus2(double modulus2) {
		this.modulus2 = modulus2;
	}
	private int op;
	private double modulus1 = 1.0;
	private double modulus2 = 1.0;

	public OptimusDoubleCalculator(){};
	public OptimusDoubleCalculator(char op){
		this.op = op;
	};
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(op);
		new DoubleWritable(this.modulus1).write(out);
		new DoubleWritable(this.modulus2).write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.op = in.readInt();
		DoubleWritable w = new DoubleWritable();
		w.readFields(in);
		this.modulus1 = w.get();
		w.readFields(in);
		this.modulus2 = w.get();
	}

	@Override
	public Object calcOne(Object obj1, Object obj2) {
		// TODO Auto-generated method stub
		Double o1 = (Double)obj1;
		Double o2 = (Double)obj2;
		o1*= this.modulus1;
		o2*= this.modulus2;
		switch(op)
		{
		case '+':
			return o1+o2;
		case '-':
			return o1-o2;
		case '*':
			return o1*o2;
		case '/':
			if(o2 == 0)
				return Double.MAX_VALUE;
			return o1/o2;
		default:
				break;
		}
		return o1+o2;
	}

	@Override
	public Object []calcArray(Object a1[], Object []a2)
	{
		switch(op)
		{
		case '+':
	//		return calcArrayAdd(a1,a2);
		case '-':
	//		return calcArraySub(a1,a2);
		case '*':
	//		return calcArrayMul(a1,a2);
		default:
				break;
		}
		return null;
	}
	
	private double[] calcArrayAdd(double[] a1, double[] a2) {
		// TODO Auto-generated method stub
		double [] a3 = new double [a1.length];
		for(int i = 0; i < a1.length; i++)
		{
			a3[i] = a1[i]*this.modulus1+a2[i]*this.modulus2;
		}
		return a3;
	}
	private double[] calcArraySub(double[] a1, double[] a2) {
		// TODO Auto-generated method stub
		double [] a3 = new double [a1.length];
		for(int i = 0; i < a1.length; i++)
		{
			a3[i] = a1[i]*this.modulus1-a2[i]*this.modulus2;
		}
		return a3;
	}
	private double[] calcArrayMul(double[] a1, double[] a2) {
		// TODO Auto-generated method stub
		double [] a3 = new double [a1.length];
		for(int i = 0; i < a1.length; i++)
		{
			a3[i] = a1[i]*a2[i]*this.modulus1*this.modulus2;
		}
		return a3;
	}
	
}
