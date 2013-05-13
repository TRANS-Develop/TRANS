package TRANS.test;

import java.util.Set;

import TRANS.Array.DataChunk;

public class CalculatorTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int []start = {0,0,0};
		int []off = {240,4200,3600};
		int []stride = {12,10,10};
		int []rstart = {180,840,0};
		int []roff = {60,420,360};
		DataChunk chunk = new DataChunk(off,stride);
		Set<DataChunk> c = chunk.getAdjacentChunks(rstart, roff);
		for(DataChunk cs:c)
		{
			System.out.println(cs.toString());
		}
	}

}
