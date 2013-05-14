package TRANS.Client.creater;

import java.io.IOException;
import java.util.Random;

import TRANS.Array.DataChunk;


public class OptimusRandomScanner  implements OptimusScanner{

	private Class<?> type = null;
	public OptimusRandomScanner(Class<?> type){
		this.type = type;
	}
	@Override
	public Object[] readChunkData(DataChunk chunk, String name) {
		Object []data = new Object [chunk.getSize()];
		Random r = new java.util.Random();
		if(type.equals(Double.class))
		{
			for(int i =  0; i < data.length; i++)
			{
				data[i] = r.nextDouble();
			}
		}else if(type.equals(Float.class))
		{
			for(int i =  0; i < data.length; i++)
			{
				data[i] = r.nextFloat();
			}
		}else if(type.equals(Integer.class))
		{
			for(int i =  0; i < data.length; i++)
			{
				data[i] = r.nextFloat();
			}
		}
		return data;
	}
	@Override
	public Class<?> getElementType(String name) throws IOException {
		// TODO Auto-generated method stub
		return this.type;
	}

}
