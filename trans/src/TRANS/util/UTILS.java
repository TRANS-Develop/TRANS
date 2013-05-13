package TRANS.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Vector;

public class UTILS {
	static public int[] getCordinate(String tmp) {
		String[] tmps = tmp.split(",");
		int[] ret = new int[tmps.length];
		int i = 0;
		for (String s : tmps) {
			ret[i++] = Integer.parseInt(s);
		}
		return ret;
	}
	static public Host RandomHost(String [] host,Vector<Host> hs) throws UnknownHostException
	{
		String hostname = InetAddress.getLocalHost().getHostName();
		int r = -1;
		for( int i = 0; i < host.length; i++)
		{
			if(host[i] == hostname)
			{
				r = i;
				break;
			}
		}
		Random ra = new Random();
		while( r < 0 )
		{
			r = ra.nextInt()%host.length;
		}
		return hs.get(r);
	}
}
