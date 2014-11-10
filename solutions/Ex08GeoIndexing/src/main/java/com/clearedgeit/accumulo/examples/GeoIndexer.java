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
import com.clearedgeit.accumulo.examples.util.GeoUtil;

public class GeoIndexer extends Configured implements Tool {

	public static class GeoMapper extends Mapper<Key, Value, Text, Text> {

		// The mapper's output key
		private Text outkey = new Text();

		// The mapper's output value
		private Text outvalue = new Text();

		private static final Text TWEET_CF = new Text("tweet");
		private static final Text LAT_CQ = new Text("lat");
		private static final Text LON_CQ = new Text("lon");
		private String latitude, longitude, latID, lonID, encodedLocation;

		public void map(Key key, Value value, Context context)
				throws IOException, InterruptedException {

			// If the key's CF is tweet and CQ is lat
			if (key.getColumnFamily().equals(TWEET_CF)
					&& key.getColumnQualifier().equals(LAT_CQ)) {
				// Save off the latitude and it's associated tweet ID
				latID = key.getRow().toString();
				latitude = value.toString();
			}

			// If the key's CF is tweet and CQ is lon
			if (key.getColumnFamily().equals(TWEET_CF)
					&& key.getColumnQualifier().equals(LON_CQ)) {
				// Save off the longitude and it's associated tweet ID
				lonID = key.getRow().toString();
				longitude = value.toString();
			}

			// If both the latitude and longitude values are not null
			if (latitude != null && longitude != null) {
				// sanity check that they are from the same tweet ID
				if (latID.equals(lonID)) {
					// Encode the location using the GeoUtil
					encodedLocation = GeoUtil.encodeLocation(latitude,
							longitude);

					// Set the outgoing key and value
					outkey.set(encodedLocation);
					outvalue.set(latID);

					// Write the key/value pair to context
					context.write(outkey, outvalue);
				}

				// Clear the latitude and longitude values
				latitude = longitude = null;
			}
		}
	}

	public static class GeoReducer extends Reducer<Text, Text, Text, Mutation> {
		// A record to be entered into the database
		private Mutation record;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// Create a new Mutation with the input key
			record = new Mutation(key);

			// Loop through all of the values
			for (Text tweetID : values) {
				// (location, "tid", tweet ID, "")
				record.put("tid", tweetID.toString(), "");
			}

			// Write the key/value pair to context
			context.write(null, record);
		}
	}

	/**
	 * The run method which sets the configuration and starts the MapReduce job
	 */
	public int run(String[] args) throws Exception {

		// Set the instance and ZooKeeper hosts
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.withInstance(Constants.INSTANCE);
		clientConfig.withZkHosts(Constants.ZOOKEEPERS);

		// Create and initialize a MapReduce Job
		Job job = Job.getInstance(getConf(), "Ex8GeoIndexing");
		job.setJarByClass(GeoIndexer.class);

		// Set the AccumuloInputFormat's connector info, scan
		// authorizations, ZooKeeper instance, and input table name
		AccumuloInputFormat.setConnectorInfo(job, Constants.USER_NAME,
				new PasswordToken(Constants.USER_PASS));
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		AccumuloInputFormat.setZooKeeperInstance(job, clientConfig);
		AccumuloInputFormat.setInputTableName(job, Constants.TWITTER_TABLE);

		// Set the AccumuloOutputFormat's connector info, create tables,
		// default table name, and ZooKeeper instance
		AccumuloOutputFormat.setConnectorInfo(job, Constants.USER_NAME,
				new PasswordToken(Constants.USER_PASS));
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setDefaultTableName(job,
				Constants.TWEET_LOCATION_INDEX_TABLE);
		AccumuloOutputFormat.setZooKeeperInstance(job, clientConfig);

		// Set the map and reduce classes
		job.setMapperClass(GeoMapper.class);
		job.setReducerClass(GeoReducer.class);

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
		System.exit(ToolRunner.run(new Configuration(), new GeoIndexer(), args));
	}
}