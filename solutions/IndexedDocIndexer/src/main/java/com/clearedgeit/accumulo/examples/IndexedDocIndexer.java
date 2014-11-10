package com.clearedgeit.accumulo.examples;

import java.io.IOException;
import java.util.Random;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.LocalEnvUtil;

public class IndexedDocIndexer extends Configured implements Tool {

  private static boolean USE_MINI_ACCUMULO = true;
	private static String userName 			= "root";			// The name of the Accumulo user 
	private static String userPass			= "trainingUser";	// The users password
	private static String twitterDataTable 	= "TwitterData";	// The table containing twitter information
	private static String tweetDocIndex	    = "TweetDocIndex";  // The table containing the document partitioned index
	private static String instanceName 		= "localAccumulo";	// The Accumulo instance name (stored in ZooKeeper)
	private static String zookeepers 		= "localhost";		// A comma separated list of ZooKeeper servers
	private static Boolean createTables		= true;				// Allow reducer to create tables that don't exist
	

	public static class TweetMapper extends Mapper<Key, Value, Text, Text> {
		
		private Text keyOut = new Text();	// The mapper's output key
		private Text valueOut = new Text();	// The mapper's output value
		
		// Random Number generator to create partition IDs
		Random r = new Random();
		
		private static final int MAX_PARTITION = 10;
		
		// Some regexes to help clean up the text in tweets
		private String splitRegex = "[\\W&&[^@#':\\/\\.]]+";
		private String urlRegex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
		private String tweetRegex = "[\\W&&[^@#]]*([@#]?\\w+)[\\W]*";
		private String uglyTextRegex = "[\\W]*(\\w+[']?\\w*)[\\W]*";
		
		
		public void map(Key keyIn, Value valueIn, Context context) 
				throws IOException, InterruptedException {
            
            
			// Only process records containing tweet text
			if(keyIn.getColumnFamily().equals(new Text("text"))) {
            	
			    int docPartition = r.nextInt(MAX_PARTITION);
			    
				// Split the text into tokens
				String[] tokens = valueIn.toString().split(splitRegex);

				// Process each word and add it as a key with its tweet ID as a value
            	for(String token: tokens) {
            		
            		// Omit zero length tokens and tokens only containing special characters
            		if(token.length() != 0 && !token.matches("[\\W]*")) {
	            		
	            		// Filter some of the garbage
	            		if(!token.matches(urlRegex)){
	            			if(token.matches(tweetRegex)) {
	            				token = token.replaceAll(tweetRegex, "$1");
	            			} else {
	            				token = token.replaceAll(uglyTextRegex, "$1");
	            			}
	            		}

	            		// Set the outgoing key and value
	            		keyOut.set(Integer.toString(docPartition));
	            		String colFam = token.toLowerCase();
	            		String colQual = keyIn.getRow().toString();
	            		valueOut.set(colFam + ":" + colQual);
	            		
	            		// Create an output Key Value pair
	            		context.write(keyOut, valueOut);
            		}
            	}
            }
		}
	}
	

	public static class TweetReducer extends Reducer<Text, Text, Text, Mutation> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			Mutation record = new Mutation(key);	// A record to be entered into the database
			
			// Loop through all of the values
			for(Text value : values) {

				// ----------(partitionID, term, docId, Empty)----------
				String[] column = value.toString().split(":");
				record.put(column[0],   // ColFamily = term
						column[1],      // ColQual is docId
						"");            // Value is EMPTY
				
			}
			// Create an output Key Value pair
            // null as the key specifies that we want to write to 
            // our default Accumulo table specified in our run method
            context.write(null, record);
		}
	}
	

	/**
	 * The run method which sets the configuration and starts the MapReduce job
	 */
	public int run(String[] args) throws Exception {
	  
	  if(USE_MINI_ACCUMULO){
	    Connector connector = LocalEnvUtil.getConnector(userPass);
	    userName = "root";
	    instanceName = connector.getInstance().getInstanceName();
	    zookeepers = connector.getInstance().getZooKeepers();
	  }
		
		// Create and initialize a MapReduce Job
		Job job = Job.getInstance(getConf(), "tweetIndexer");
		job.setJarByClass(IndexedDocIndexer.class);
		
		
		// Set the AccumuloInputFormat so the mapper can read from Accumulo
		
		AccumuloInputFormat.setConnectorInfo(job,
		    userName, 
		    new PasswordToken(userPass));
		
		AccumuloInputFormat.setInputTableName(job, twitterDataTable);
		
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.withInstance(instanceName);
		clientConfig.withZkHosts(zookeepers);
		
		AccumuloInputFormat.setZooKeeperInstance(job, clientConfig);
		
		
		AccumuloOutputFormat.setConnectorInfo(job, 
		    userName,
		    new PasswordToken(userPass));
		
		AccumuloOutputFormat.setCreateTables(job, createTables);
		
		AccumuloOutputFormat.setDefaultTableName(job, tweetDocIndex);
		
		AccumuloOutputFormat.setZooKeeperInstance(job, clientConfig);
		
		
		// Set the map and reduce classes
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(TweetReducer.class);
		
		// Set the output key and value class for the mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Set the output key and value class for the reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		
		// Set the InputFormat and OutputFormat for the job
		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		
		// Run the MapReduce job and return 0 for success, 1 otherwise
		return job.waitForCompletion(true) ? 0 : 1;
	}
	

	public static void main(String[] args) throws Exception {
		
		// Call the run method to launch the MapReduce Job
		int result = ToolRunner.run(new Configuration(USE_MINI_ACCUMULO), new IndexedDocIndexer(), args);		
		System.exit(result);
	}
}
