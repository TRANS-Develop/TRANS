package TRANS.Client;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.jdom2.JDOMException;

import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusZone;
import TRANS.Array.ZoneID;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDefault;

public class ArrayDeleter {
	static public  void main(String args[]) throws ParseException, IOException, WrongArgumentException, JDOMException
	{
		CommandLineParser parser = new PosixParser();
		
		Options options = new Options();
		HelpFormatter f = new HelpFormatter();
		 
		options.addOption("h",false,"Print The help infomation");
		options.addOption("c", true, "Configuration directory of catalog");
		options.addOption("n", true, "Chunk Strategy of the partition");

		CommandLine cmd = parser.parse(options, args);
		
		if( cmd.hasOption("h"))
		{
             f.printHelp("NetCDF Loader", options);
             System.exit(-1);
		}
		String confDir = cmd.getOptionValue("c",OptimusDefault.OPTIMUSCONFIG);
		String name = cmd.getOptionValue("n");
		if(name == null)
		{
			f.printHelp("Array Deleter", options);
			System.out.println("No Array name specified");
			System.exit(-1);
		}
		String zoneArray[] = name.split("\\.");
		String zone = zoneArray[0];
		String aname = zoneArray[1];
		ZoneClient zcreater = new ZoneClient(new OptimusConfiguration(confDir));
		OptimusZone z = zcreater.openZone(zone);
		if(z == null)
		{
			System.out.println("Unknown Zone");
			System.exit(-1);
		}
		OptimusCatalogProtocol ci = zcreater.getCi();
		OptimusArray a = ci.openArray(z.getId(), new Text(aname));
		BooleanWritable b = ci.deleteArray(a.getId());
		if(b!= null && b.get() == true)
		{
			System.out.println("Deleted Array Successfully");
		}else{
			System.out.println("Delete failure");
		}
	}

}
