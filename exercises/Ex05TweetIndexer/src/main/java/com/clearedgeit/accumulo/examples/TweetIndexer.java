package com.clearedgeit.accumulo.examples;

import java.io.IOException;

import org.apache.accumulo.core.client.ClientConfiguration;
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

import com.clearedgeit.accumulo.examples.util.Constants;

public class TweetIndexer extends Configured implements Tool {

	public static class TweetMapper extends Mapper<Key, Value, Text, Text> {

		private Text keyOut = new Text(); // The mapper's output key
		private Text valueOut = new Text(); // The mapper's output value
		private static final Text TWEET_COLUMN_FAMILY = new Text("tweet");
		private static final Text TEXT_COLUMN_QUALIFIER = new Text("text");

		// Some regexs to help clean up the text in tweets
		private String splitRegex = "[\\W&&[^@#':\\/\\.]]+";
		private String urlRegex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
		private String tweetRegex = "[\\W&&[^@#]]*([@#]?\\w+)[\\W]*";
		private String uglyTextRegex = "[\\W]*(\\w+[']?\\w*)[\\W]*";

		public void map(Key key, Value value, Context context)
				throws IOException, InterruptedException {

			// TODO If the CF equals tweet and the CQ equals text
			if (...) {

				// Split the text into tokens
				String[] tokens = value.toString().split(splitRegex);

				// For each word in the text
				for (String token : tokens) {
					// Omit zero length tokens and tokens only containing
					// special characters
					if (token.length() != 0 && !token.matches("[\\W]*")) {
						// Filter some of the garbage
						if (!token.matches(urlRegex)) {
							if (token.matches(tweetRegex)) {
								token = token.replaceAll(tweetRegex, "$1");
							} else {
								token = token.replaceAll(uglyTextRegex, "$1");
							}
						}

						// TODO Set the outgoing key and value

						// TODO Write an output key value pair
					}
				}
			}
		}
	}

	public static class TweetReducer extends
			Reducer<Text, Text, Text, Mutation> {
		// A record to be entered into the database
		private Mutation record;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// TODO Loop through all of the values
				// TODO (word, "index", tweet ID, "")

				// TODO Create an output Key Value pair
				// null as the key specifies that we want to write to
				// our default Accumulo table specified in our run method
			
		}
	}

	/**
	 * The run method which sets the configuration and starts the MapReduce job
	 */
	public int run(String[] args) throws Exception {

		// TODO Set the zookeeper hosts and instance name in the client config
		ClientConfiguration clientConfig = null;

		// Create and initialize a MapReduce Job
		Job job = Job.getInstance(getConf(), "Ex5TweetIndexer");
		job.setJarByClass(TweetIndexer.class);

		// TODO Set the AccumuloInputFormat's connector info, scan authorizations,
		// input table name, and zookeeper instance so the mapper can read from
		// Accumulo

		// TODO Set the AccumuloOutputFormat's connector info, create tables, default
		// table name, and zookeeper instance


		// Set the map and reduce classes
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(TweetReducer.class);

		// Set the output key and value class for the mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set the output key and value class for the reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		// TODO Set the InputFormat and OutputFormat for the job

		// Run the MapReduce job and return 0 for success, 1 otherwise
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// Call the run method to launch the MapReduce Job
		System.exit(ToolRunner.run(new Configuration(), new TweetIndexer(),
				args));
	}
}
