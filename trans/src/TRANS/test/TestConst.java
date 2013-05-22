package TRANS.test;

import java.util.Random;

public class TestConst {
	static int arrays = 0;

	static public String testZoneName="out";
	static public String testArrayName="26";
	static public int [] srcStart={0,0,0,0};
	static public int [] vsize={8,8,8,8};
	static public int [] psize={8,8,8,8};
	static public int [] sshape={8,8,8,8};
	static public int [] dstShape1={8,8,8,1};
	static public int [] dstShape2={8,8,8,1};
	static public int [] dstShape3={8,8,8,1};
	static public int [] overlap={2,2};
	
	static public int [] stride ={2,2};
	static public String nextArrayName()
	{
		Random r = new Random();
		return (testArrayName = testArrayName+(arrays++)+'_'+r.nextInt());
	}
	static public void setTestSuite(String name)
	{
		//TODO
	}
}
