package Yelp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.filecache.*; 

@SuppressWarnings("deprecation")
public class Question4 extends Configured implements Tool
{
	
	public static class Map4 extends Mapper<LongWritable, Text, Text, Text> {
		  
        private static HashSet<String> businessSet = new HashSet<String>();
        private static HashMap<String, Double> userToAvgRatingMap = new HashMap<String, Double>();
    	private static HashMap<String, Double> userToCountMap = new HashMap<String, Double>();

        private BufferedReader brReader;
        private String businessId = "";
        private Text outKey = new Text("");
        private Text outValue = new Text("");
        private FileSystem fs = null;
  
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
                Path[] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                // get the path from distributed cache
                  for (Path filePath : cacheFile) {
                        if (filePath.getName().toString().trim().equals("business.csv")) {
                        	// find the path of business.csv file    
                        	setHashSet(filePath, context);  
                        }
                  }
	     }
                
        private void setHashSet(Path filePath, Context context) throws IOException {
                String line = "";
                FileSystem fs = FileSystem.getLocal(context.getConfiguration());
                brReader = new BufferedReader(new InputStreamReader(fs.open(filePath)));;   // now read the file suing Buffered reader
                while ((line = brReader.readLine()) != null) {
                	String businessData[] = line.toString().split("::");   // split the line
                    if(businessData[1].contains("Palo Alto")) //check for Palo Alto Location
                    { 
                    	businessSet.add(businessData[0].trim());   //store the business ids of Palo Alto location
                    }
               }      
        }
  
  
        @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] data=value.toString().split("::");  //split the input line
			double averageRating = 0;
			
				if(data.length>3 && businessSet.contains(data[2])){    // businesses in PaloAlto location
					if(!userToAvgRatingMap.containsKey(data[1])){
						userToAvgRatingMap.put(data[1], Double.parseDouble(data[3]));  // store user and rating
						userToCountMap.put(data[1],1.0 );  // store count of businesses rated by a single user
					}
					else{
						userToAvgRatingMap.put(data[1], userToAvgRatingMap.get(data[1]+ Double.parseDouble(data[3])));  //adds up the rating of single suer
						userToCountMap.put(data[1], userToCountMap.get(data[1])+1); // addds up the count of businesses rated by single user
					}
				}
		}	
		@Override
		public void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)	throws IOException, InterruptedException {

			//iterate for each user , calculate average and write the final output
			for (Entry<String, Double> entry : userToAvgRatingMap.entrySet()) {
				double avg = 0;
				double count = userToCountMap.get(entry.getKey()); //gets the count for that user
				if(entry.getValue()!= null)
					avg = entry.getValue() / count;	 //calculate average 
				else 
					avg = 0;
				context.write(new Text(entry.getKey()), new Text(String.valueOf(avg))); //write the output user and average rating 
			}
		}
	}
        
 
	
	
	
    public static void main( String[] args ) throws Exception
    {
        int exitCode = ToolRunner.run(new Configuration(),new Question4(), args);
        System.exit(exitCode);
    }
    
	@Override
    public int run(String[] args) throws Exception {
        if(args.length !=3 ){
                System.err.println("IUsage: hadoop jar <Input File1>(business.csv) <InputFile2>(review.csv) <OutputFile>");
                System.exit(2);
        }
        Job job = new Job(getConf());
        Configuration conf = job.getConfiguration();
        job.setJobName("MapSideJoin using distributedCache");

        job.setJarByClass(Question4.class);
        //add business.csv to distributed cache
        DistributedCache.addCacheFile(new URI(args[0]), job.getConfiguration());
       
        //set input and output
        FileInputFormat.addInputPath(job,new Path(args[1]) );
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(Map4.class);
        job.setNumReduceTasks(0);
         
        boolean success = job.waitForCompletion(true);
            return success ? 0 : 1;
     }
	
}