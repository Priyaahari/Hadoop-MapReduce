package Yelp;
//package yelp;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.LongWritable;


public class Question3 {
	public static class MultipleMap1 extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line= value.toString().split("::");  //split the input line in business.csv
		    String str=line[1]+"xy:"+line[2];  //concatenate address and category and store
			context.write(new Text(line[0]), new Text(str));
		}
	}
	
	public static  class MultipleMap2 extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line= value.toString().split("::");  //split the input line in review.csv
			context.write(new Text(line[2]), new Text(line[3]));  //write business id and stars 
			
		}
		
	}
	
	public static class Reduce3 extends Reducer<Text,Text,Text,Text>
	{
		String merge = "";
		HashMap<String,Double> countMap=new HashMap<String,Double>();
		HashMap<String, Text> finalMap = new HashMap<String, Text>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String address = "";
			String category = "";
			int count = 0;
			double sum = 0;

			for (Text value : values) {
				// split the address and category concatenated in MultipleMap1
				String[] words = value.toString().split("xy:");
				if (words.length == 2) {  //check if MulitpleMap1
					address = words[0];
					category = words[1];
				} else {  // output of MultipleMap2
					sum += Double.parseDouble(words[0]);
					count++;
				}
			}
			
			//calculate avg and store in HashMap
			double avg = sum / count;
			if(!Double.isNaN(avg))
			{
				String merge = address + " " + category + " "+ avg;
				countMap.put(key.toString(), avg);      
				finalMap.put(key.toString(), new Text(merge));	
			}

		}
		
		public void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			//sort the Map to find the top 10 and finally print them
			TreeMap<String, Double> final_map = new TreeMap<String, Double>(new Sort(countMap));
			final_map.putAll(countMap);
			int i = 0;
			for (Entry<String, Double> entry : final_map.entrySet()) {
				if(i<10)
				{
				context.write(new Text(entry.getKey()), new Text(finalMap.get(entry.getKey())));
				i++;
				}
				else
					break;
			}
			
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception
	{
		
		Configuration conf=new Configuration();
		String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 3 ){
			System.err.println ("Usage: hadoop jar  <InputFile1>(business.csv) <InputFile2>(review.csv) <OutputFile>");
			System.exit(2);
		}
		Path p1=new Path(otherArgs[0]);
		Path p2=new Path(otherArgs[1]);
		Path p3=new Path(otherArgs[2]);
		// create a job with name "Reducer Join"
		Job job = Job.getInstance(conf,"Yelp Reduce side Join");
		job.setJarByClass(Question3.class);
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MultipleMap1.class);
		MultipleInputs.addInputPath(job,p2, TextInputFormat.class, MultipleMap2.class);
		job.setReducerClass(Reduce3.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, p3);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
