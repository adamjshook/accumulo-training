package com.clearedgeit.accumulo.examples;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.SortedSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import com.clearedgeit.accumulo.examples.util.Constants;
import com.clearedgeit.accumulo.examples.util.FileSampling;

public class BulkTweetIngester extends Configured implements Tool {

	public static class TweetMapper extends
			Mapper<LongWritable, Text, Key, Value> {

		private Key outkey = new Key();
		private Value outvalue = new Value();

		public void map(LongWritable keyIn, Text valueIn, Context context)
				throws IOException, InterruptedException {

			try {
				// Create a status object based off of the JSON object read
				Object o = TwitterObjectFactory
						.createObject(valueIn.toString());
				Status tweet = null;

				if (o instanceof Status) {
					tweet = (Status) o;

					String tweetId = Long.toString(tweet.getId());

					// Only process if the tweet is in English
					if (tweet.getUser().getLang().equals("en")) {
						// (tweetId, "tweet", "text", text)
						outkey.set(new Key(tweetId, "tweet", "text"));
						outvalue.set(tweet.getText().getBytes());
						context.write(outkey, outvalue);

						// (tweetId, "tweet", "created_at", creation_date)
						outkey.set(new Key(tweetId, "tweet", "created_at"));
						outvalue.set(tweet.getCreatedAt().toString().getBytes());
						context.write(outkey, outvalue);
						
						// If tweet geo location is not null
						if (tweet.getGeoLocation() != null) {
							// (tweetId, "tweet", "lat", geoLoc.lat)
							outkey.set(new Key(tweetId, "tweet", "lat"));
							outvalue.set(Double.toString(
									tweet.getGeoLocation().getLatitude())
									.getBytes());
							context.write(outkey, outvalue);

							// (tweetId, "tweet", "lon", geoLoc.lon)
							outkey.set(new Key(tweetId, "tweet", "lon"));
							outvalue.set(Double.toString(
									tweet.getGeoLocation().getLongitude())
									.getBytes());
							context.write(outkey, outvalue);
						}

						// (tweetId, "user", "id", user.id)
						outkey.set(new Key(tweetId, "user", "id"));
						outvalue.set(Long.toString(tweet.getUser().getId())
								.getBytes());
						context.write(outkey, outvalue);

						// (tweetId, "user", "screenname", user.screenName)
						outkey.set(new Key(tweetId, "user", "screenname"));
						outvalue.set(tweet.getUser().getScreenName().getBytes());
						context.write(outkey, outvalue);

						// (tweetId, "user", "lang", user.lang)
						outkey.set(new Key(tweetId, "user", "lang"));
						outvalue.set(tweet.getUser().getLang().getBytes());
						context.write(outkey, outvalue);
					}
				}
			} catch (Exception e) {
				// Display an error if the twitter data is malformed
				System.out.println("Error: " + e.getMessage() + '\n'
						+ e.getLocalizedMessage());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err
					.println("Usage: accumulo-bulk-ingest-example.jar <input_dir> <output_dir>");
			return 1;
		}

		// Set the location of the twitter data
		Path inputDir = new Path(args[0]);
		// The base working directory
		Path baseWorkingDir = new Path(args[1]);
		// The location of the actual RFiles
		Path rfileOutput = new Path(baseWorkingDir, "files");
		// A working directory to sample the input to pre-split the table
		Path samplerWorkingDir = new Path(baseWorkingDir, "sampledir");
		// An HDFS file containing the predetermined splits
		Path splitsFile = new Path(baseWorkingDir, "splits.txt");
		// A directory for the import tool to store the files that failed to
		// load
		Path failures = new Path(baseWorkingDir, "failures");

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		// Create and initialize a MapReduce Job
		Job job = Job.getInstance(conf);
		job.setJobName("Ex7BulkTweetIngest");
		job.setJarByClass(BulkTweetIngester.class);

		// Set the TextInputFormat input paths
		TextInputFormat.setInputPaths(job, inputDir);

		// Set the AccumuloFileOutputFormat output path
		AccumuloFileOutputFormat.setOutputPath(job, rfileOutput);

		// Set the map and reduce classes
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(Reducer.class);

		// Set the output key and value class
		job.setOutputKeyClass(Key.class);
		job.setOutputValueClass(Value.class);

		// Set the InputFormat and OutputFormat for the job
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(AccumuloFileOutputFormat.class);

		// Set the job to use the KeyRangePartitioner
		job.setPartitionerClass(KeyRangePartitioner.class);

		// Configure the ZooKeeper instance and the Accumulo connection
		// objects
		ZooKeeperInstance instance = new ZooKeeperInstance(Constants.INSTANCE,
				Constants.ZOOKEEPERS);
		Connector connector = instance.getConnector(Constants.USER_NAME,
				new PasswordToken(Constants.USER_PASS));

		// Delete the table if it already exists
		if (connector.tableOperations().exists(Constants.TWITTER_TABLE)) {
			connector.tableOperations().delete(Constants.TWITTER_TABLE);
		}

		// Get 10 split points from the FileSampling utility with a .01
		// sample rate
		SortedSet<Text> splitPoints = FileSampling.getSplits(conf, inputDir,
				samplerWorkingDir, .01, 10);

		// Log the splits to stdout and write them to a splits file in HDFS
		PrintWriter out = new PrintWriter(fs.create(splitsFile));

		for (Text split : splitPoints) {
			System.out.println("Adding split " + split);
			out.println(new String(
					Base64.encodeBase64(TextUtil.getBytes(split))));
		}

		// Close the HDFS file
		out.close();

		// Create the table and add the split points
		connector.tableOperations().create(Constants.TWITTER_TABLE);
		connector.tableOperations().addSplits(Constants.TWITTER_TABLE,
				splitPoints);

		// Set the number of reducers based on the number of splits + 1
		job.setNumReduceTasks(splitPoints.size() + 1);

		// Set the range partitioner's split file
		KeyRangePartitioner.setSplitFile(job, splitsFile.toString());

		// Run the MapReduce job
		int code = job.waitForCompletion(true) ? 0 : 1;

		// if the job was successful
		if (code == 0) {
			// Open the permissions so the accumulo user can access the files
			fs.setPermission(rfileOutput, new FsPermission(FsAction.ALL,
					FsAction.ALL, FsAction.ALL));

			// Create a failures directory in HDFS and set open permissions
			fs.mkdirs(failures);
			fs.setPermission(failures, new FsPermission(FsAction.ALL,
					FsAction.ALL, FsAction.ALL));

			// Import the raw RFiles into Accumulo
			connector.tableOperations().importDirectory(
					Constants.TWITTER_TABLE, rfileOutput.toString(),
					failures.toString(), false);
		}
		return code;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(true),
				new BulkTweetIngester(), args));
	}
}
