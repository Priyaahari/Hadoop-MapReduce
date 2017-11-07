package Yelp;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


public class Question1 extends Configured {

	public static class MapQ1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text friendsList = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String input_line = value.toString();
			String[] input_split = input_line.split("\t");
			String user = input_split[0];   //user
			Long userL = Long.parseLong(user);
			
			// split the input line
			if (input_split.length == 2) {
				String friendsList1 = input_split[1];
				// creating the array of all friends of the user
				String[] friendsListArray = input_split[1].split(","); 
				
				// iterate for every friend of the user and create key pair
				for(String friend:friendsListArray){
					friendsList.set(friendsList1);					
					ArrayList<Integer> keyList = new ArrayList<Integer>();
				
					if (userL.compareTo(Long.parseLong(friend)) < 0) {
						keyList.clear();
						keyList.add(Integer.parseInt(userL.toString()));
						keyList.add(Integer.parseInt(friend));
						context.write(new Text(StringUtils.join(",", keyList)), friendsList);
					
					} else {
						keyList.clear();
						keyList.add(Integer.parseInt(friend));
						keyList.add(Integer.parseInt(userL.toString()));
						context.write(new Text(StringUtils.join(",", keyList)), friendsList);
						
					}
				}
			}
		}
	}

	public static class ReduceQ1 extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] groupedFriends = new String[2];
			int current = 0;
			for (Text v : values) {
				groupedFriends[current++] = v.toString();
			}

			if (null != groupedFriends[0]) {     // user 1
				groupedFriends[0] = groupedFriends[0].replaceAll("[^0-9,]", "");

			}
			if (null != groupedFriends[1]) {   // user 2
				groupedFriends[1] = groupedFriends[1].replaceAll("[^0-9,]", "");
			}

			// creating the array with friends of user1 and user 2
			String[] list1 = groupedFriends[0].split(",");   
			String[] list2 = groupedFriends[1].split(",");	
			ArrayList<Integer> alist1=new ArrayList<Integer>();
			ArrayList<Integer> finalList = new ArrayList<Integer>();
			
			// adding friends of user1 into array list so that it will be used to compare with friends of user2 
			if(null != groupedFriends[0]){
				for (String str:list1){
					alist1.add(Integer.parseInt(str));    
				}
			}
			
			//now iterate through the friends of user2 to find mutual friends
			if(null != groupedFriends[1]){
				for(String str:list2){
					if(alist1.contains(Integer.parseInt(str))){
						finalList.add(Integer.parseInt(str));  // adding into mutual friends list
					}
				}
			}
		
			
			// printing only for the user pairs given in the question
			if(key.toString().contentEquals("0,4") || key.toString().contentEquals("20,22939") || key.toString().contentEquals("1,29826") || 
					key.toString().contentEquals("6222,19272") || key.toString().contentEquals("28041,28056")){
							context.write(new Text(key.toString()), new Text(StringUtils.join(",", finalList)));
			}

		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop jar <InputFile> <OutputFile> ");
            System.exit(2);
        }
	  	// create a job with name "MutualFriends"
	    Job job = new Job(conf, "Mutual Friends");

	    //set Map and Reducer classes
	    
	    job.setJarByClass(Question1.class);
		job.setMapperClass(MapQ1.class);
		job.setReducerClass(ReduceQ1.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	
}