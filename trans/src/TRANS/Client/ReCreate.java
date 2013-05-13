package TRANS.Client;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import TRANS.Array.DataChunk;

import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class ReCreate {
	public static List<File> getNc(String p)
	{
		
		File dir = new File(p);
		if(!dir.isDirectory())
		{
			return null;
		}
		List<File> files = new LinkedList<File>();
		File [] fs = dir.listFiles();
		for(File f:fs)
		{
			String fname = f.getAbsolutePath();
			if(f.isFile()&& fname.endsWith(".nc"))
			{
				files.add(f);
			}else if(f.isDirectory() && !fname.equals(".")&&!fname.equals("..")){
				files.addAll(getNc(fname));	
			}
		}
		return files;
		
	}
	public static void main(String[] args) throws IOException, InvalidRangeException {
		
		if(args.length < 2)
		{
			System.out.println("Usage: InputDir ouputdir");
			System.exit(-1);
		}
		String path = args[0];
		String output = args[1];
		System.out.println("Input Path:" + args[0]);
		System.out.println("Input Path:" + args[1]);
		
		File dir = new File(path);
		if (!dir.isDirectory()) {
			System.exit(-1);
		}
		List<File> files = getNc(path);
		System.out.println("Total File:"+files.size());
		int num = 0;
		for (File file : files) {
			
			if(file.isDirectory())
			{
				
			}
			NetcdfFile nc = NetcdfFile.open(file.getPath());
			List<Variable> vars = nc.getVariables();
			int l = 0;
			Variable large = null;
			for (Variable var : vars) {
				int len = var.getShape().length;
				if (len > l) {
					large = var;
					l = len;
				}
			}
			System.out.println(num+"-"+Arrays.toString(large.getShape()));
			int size = 64*1024*1024;
			int tmp = 1;
			int[] shape = new int[large.getShape().length];
			for (int i = 0; i < shape.length; i++) {
				if(tmp > size)
				{
					shape[i] = 1;
				}else{
				shape[i] = large.getShape(i);
				tmp *= shape[i];
				}
			}
			
			DataChunk chunk = new DataChunk(large.getShape(), shape);
			 DataOutputStream out=new DataOutputStream(new FileOutputStream(output + "/"+(num++)
						+ large.getName()));
			 
			 BufferedWriter bufferedWriter = null;
			do {
				//bufferedWriter.write
				
				float[] data = (float[]) large.read(chunk.getStart(),
						chunk.getChunkSize()).copyTo1DJavaArray();
				byte []b = new byte[data.length * 4];
				
				
				int cur = 0;
				for (int i = 0; i < data.length; i++)
				{
					 l = Float.floatToIntBits(data[i]);
					 for (int p = 0; p < 4; p++) {
						 b[cur++] = new Integer(l).byteValue(); 
						 l = l >> 8;
					 }
					
				}
				out.write(b);
				
			} while (chunk.nextChunk());
			out.close();
			nc.close();
		}
	}

}
