package TRANS.MR.Tools;

import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import TRANS.Array.ArrayID;
import TRANS.Array.DataChunk;
import TRANS.Array.OptimusZone;
import TRANS.Client.ZoneClient;
import TRANS.MR.Binary.TransBinaryInputFormat;
import TRANS.MR.Median.StripeMedianResult;
import TRANS.MR.Median.TRANSMedianInputFormat;
import TRANS.MR.Median.combiner.TransPartitioner;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.OptimusConfiguration;
import TRANS.util.TRANSDataIterator;
import TRANS.util.UTILS;

public class Binary {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		CommandLineParser parser = new PosixParser();
		Options options = new Options();

		HelpFormatter f = new HelpFormatter();

		options.addOption("name1", true, "name of the array1 zoneName.arrayName");
		options.addOption("name2",true,"name of the array2");
		options.addOption("name3",true,"name of the array3");
		
		options.addOption("start1", true, "start point of array1");
		options.addOption("start2", true, "start point of array2");
		options.addOption("off", true,"shape to read, start.length == off.lengh");
		
		options.addOption("m1",true,"modulus for argument1");
		options.addOption("m2",true,"modulus for argument2");
		
		options.addOption("opartition", true, "Partition Strategy for the output");
		options.addOption("ostrategy", true, "Partition Strategy for the ouptut array");
		
		options.addOption("op",true,"Operation of the job");
		options.addOption("o",true,"OutputDirectory");

		options.addOption("earlybird",false,"Write The result ahead");
		
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption("h")) {
			f.printHelp("Reader printh", options);
			System.exit(-1);
		}
		String confPath = cmd.getOptionValue("c");
		if (confPath == null) {
			confPath = System.getenv("OPTIMUS_CONF");
		} 
		if(confPath == null) {
			confPath = "./conf";
		}

		String name1 = cmd.getOptionValue("name1");
		String name2 = cmd.getOptionValue("name2");
		String name3 = cmd.getOptionValue("name3");
		String start1 = cmd.getOptionValue("start1");
		String start2 = cmd.getOptionValue("start2");
		
		String off = cmd.getOptionValue("off");
		String op = cmd.getOptionValue("op");
		String outpath = cmd.getOptionValue("o");
		
		String modulus1 = cmd.getOptionValue("m1");
		if(modulus1 == null)
		{
			modulus1 = "1.0";
		}
		String modulus2 = cmd.getOptionValue("m2");
		if(modulus2 == null)
		{
			modulus2 = "1.0";
		}
		if (name1 == null || name2 == null || name3 == null|| start1 == null||off == null || start2 == null
				) {
			f.printHelp("Reader has null", options);
			System.exit(-1);
		}

		String[] names1 = name1.split("\\.");
		String[] names2 = name2.split("\\.");
		String[] names3 = name3.split("\\.");
		
		if (names1.length != 2) {

			f.printHelp("Reader zoneName.arrayName", options);
			System.exit(-1);
		}

		String zoneName3 = names3[0];
		String arrayName3 = names3[1];
		System.out.println("Creating new array "+zoneName3+"."+arrayName3);
		
	
		ZoneClient zcreater = new ZoneClient(new OptimusConfiguration(confPath));
		OptimusZone zone = zcreater.openZone(zoneName3);
		if (zone == null) {
			int []offs = UTILS.getCordinate(off);
			
			int len = offs.length;
			String[] oshape = cmd.getOptionValues("ostrategy");
			String pStep = cmd.getOptionValue("opartition");
			Vector<int[]> shapes = new Vector<int[]>();
			if (oshape == null || pStep == null) {
				f.printHelp("TRANS Median", options);
				System.exit(-1);
			}
			String[] ps = pStep.split(",");
			int[] psize = new int[len];
			for (int i = 0; i < len; i++) {
				psize[i] = Integer.parseInt(ps[i]);
			}

			for (String s : oshape) {
				
				int [] tmp = UTILS.getCordinate(s);
				shapes.add(tmp);
			}
			int[] sshape = new int[len];
			sshape[len - 1] = offs[len - 1];
			for (int i = 0; i < len - 1; i++) {
				sshape[i] = 1;
			}
			shapes.add(sshape);
			zone = zcreater.createZone(zoneName3, offs, psize, shapes);
			
		}
		OptimusCatalogProtocol ci = zcreater.getCi();
		ArrayID array = ci.createArray(zone.getId(), new Text(arrayName3),
				new FloatWritable(0));
		int []pshape = zone.getPstep().getShape();
		String p = "";
		p+=pshape[0];
		for(int i=1; i < pshape.length;i++)
		{
			p+=","+pshape[i];
		}
		DataChunk d = new DataChunk(zone.getSize().getShape(),zone.getPstep().getShape());
		
		int reducerNumber = d.getTotalChunk();
		conf.set("TRANS.INPUT.OUTPUT.PARTITION", p);
		conf.setInt("TRANS.OUTPUT.ZID",zone.getId().getId());
		conf.setInt("TRANS.OUTPUT.RNUM", zone.getStrategy().getShapes().size() -1);
		conf.setInt("TRANS.OUTPUT.AID", array.getArrayId());
		
		conf.set("TRANS.INPUT.ARG1.NAME", name1);
		conf.setFloat("TRANS.INPUT.ARG1.MODULUS", Float.parseFloat(modulus1));
		
		conf.set("TRANS.INPUT.ARG2.NAME", name2);
		conf.setFloat("TRANS.INPUT.ARG2.MODULUS", Float.parseFloat(modulus2));
		
		conf.set("TRANS.INPUT.OUTPUT.NAME", name3);
		conf.set("TRANS.INPUT.ARG1.START", start1);
		conf.set("TRANS.INPUT.ARG2.START", start2);
		conf.set("TRANS.INPUT.ARG1.OFF", off);
		
		conf.setInt("TRANS.INPUT.OP", Integer.parseInt(op));
		conf.set("TRANS.CONF.DIR", confPath);
		if(cmd.hasOption("earlybird")){
			conf.setBoolean("TRANS.EARLYBIRD", true);	
		}
		Job job = null;

		job = new Job(conf, "Median");
		job.setInputFormatClass(TransBinaryInputFormat.class);
		job.setJarByClass(Binary.class);
		//job.setCombinerClass(TRANS.MR.Median.combiner.MedianCombiner.class);
		job.setMapperClass(TRANS.MR.Binary.Mapper.TransBinaryMapper.class);
		job.setReducerClass(TRANS.MR.Binary.Reducer.TransBinaryReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TRANSDataIterator.class);
		//job.setPartitionerClass(MedianPartitioner.class);
		//job.setp
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		if(!cmd.hasOption("earlybird"))
			job.setNumReduceTasks(reducerNumber);
		// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
