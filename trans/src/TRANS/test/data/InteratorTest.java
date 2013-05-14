package TRANS.test.data;

import java.io.IOException;

import TRANS.Array.OptimusShape;
import TRANS.Data.*;
import TRANS.Exceptions.WrongArgumentException;

public class InteratorTest {

	/**
	 * @param args
	 * @throws WrongArgumentException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws WrongArgumentException, IOException {
		// TODO Auto-generated method stub
		
		Double [] d = new Double[64];
		for(int i = 0 ; i < d.length; i++ )
		{
			d[i] = new Double(i);
		}
		//public OptimusData(float []data,OptimusShape start, OptimusShape off,OptimusShape shape)
		int [] tstart = {0,0,0};
		int [] toff = {4,4,4};
		OptimusShape start = new OptimusShape(tstart);
		OptimusShape off = new OptimusShape(toff);
		OptimusData data = new OptimusData(d,start,off,null);
		int [] dstart = {3,3,0};
		int [] doff = {0,0,4};
		OptimusInterator itr = new OptimusInterator(data,new OptimusShape(dstart),new OptimusShape(doff)); 
		do
		{
			System.out.println(itr.get());
		}while(itr.next());
	}

}
