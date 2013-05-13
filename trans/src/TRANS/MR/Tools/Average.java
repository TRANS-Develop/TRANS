package TRANS.MR.Tools;

import TRANS.MR.Average.Mapper.*;
import TRANS.MR.Average.Reducer.*;
import TRANS.MR.Combiner.AverageCombiner;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import TRANS.Data.Optimus1Ddata;
import TRANS.MR.TRANSInputFormat;
import TRANS.MR.io.AverageResult;

public class Average {

	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    
	CommandLineParser parser = new PosixParser();
	Options options = new Options();
	
	HelpFormatter f = new HelpFormatter();
	 
	options.addOption("name",true,"name of the array zoneName.arrayName");
	options.addOption("start",true,"start to read");
	options.addOption("off",true,"shape to read, start.length == off.lengh");
	options.addOption("o",true,"output path");
	options.addOption("push",false,"Push calculate to datanode");
	
	CommandLine cmd = parser.parse(options, args);
	if(cmd.hasOption("h"))
	{
		f.printHelp("Reader printh", options);
		System.exit(-1);
	}
	String confPath = cmd.getOptionValue("c");
	if(confPath == null)
	{
		confPath = "./conf";
	}
	
	String name = cmd.getOptionValue("name");
	String start = cmd.getOptionValue("start");
	String off = cmd.getOptionValue("off");
	String out = cmd.getOptionValue("o");
	
	if( name == null || start == null || off == null || out == null )
	{
		f.printHelp("Reader has null", options);
		System.exit(-1);
	}
	
	String [] names = name.split("\\.");
	if(names.length != 2)
	{
	
		f.printHelp("Reader zoneName.arrayName", options);
		System.exit(-1);
	}
	String zoneName = names[0];
	String arrayName = names[1];
	
	String []starts = start.split(",");
	String []offs = off.split(",");
	if(starts.length != offs.length)
	{
		f.printHelp("Reader, start != off", options);
		System.exit(-1);
	}
    
    conf.set("TRANS.zone.name", zoneName);
    conf.set("TRANS.array.name", arrayName);
    conf.set("TRANS.range.start",start);
    conf.set("TRANS.range.offset",off);
    Job job = null;
    if(!cmd.hasOption("push"))
    {
    	job = new Job(conf, "Average");
    	
    	job.setInputFormatClass(TRANS.MR.TRANSNonPushInputFormat.class);
    	job.setMapperClass(AverageMapper.class);
    }else{
    	job = new Job(conf,"Average Push");
    	job.setInputFormatClass(TRANS.MR.TRANSPushInputFormat.class);
    	job.setMapperClass(TRANS.MR.Average.Mapper.AveragePushDownMapper.class);
    }
    job.setJarByClass(Average.class);

	job.setCombinerClass(AverageCombiner.class);
	job.setReducerClass(AverageReducer.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(AverageResult.class);
    
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    //    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(out));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
