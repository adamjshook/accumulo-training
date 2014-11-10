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

					// TODO Only process if the user's lang is English ("en")
					if (...) {
						// TODO (tweetId, "tweet", "text", text)
						outkey.set(...);
						outvalue.set(...);
						context.write(outkey, outvalue);

						// TODO (tweetId, "tweet", "created_at", creation_date)
						outkey.set(...);
						outvalue.set(...);
						context.write(outkey, outvalue);

						// TODO If tweet geo location is not null
						if (tweet.getGeoLocation() != null) {
							// TODO (tweetId, "tweet", "lat", geoLoc.lat)
							outkey.set(...);
							outvalue.set(...);
							context.write(outkey, outvalue);

							// TODO (tweetId, "tweet", "lon", geoLoc.lon)
							outkey.set(...);
							outvalue.set(...);
							context.write(outkey, outvalue);
						}

						// TODO (tweetId, "user", "id", user.id)
						outkey.set(...);
						outvalue.set(...);
						context.write(outkey, outvalue);

						// TODO (tweetId, "user", "screenname", user.screenName)
						outkey.set(...);
						outvalue.set(...);
						context.write(outkey, outvalue);

						// TODO (tweetId, "user", "lang", user.lang)
						outkey.set(...);
						outvalue.set(...);
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

		// TODO Set the TextInputFormat input paths

		// TODO Set the AccumuloFileOutputFormat output path

		// Set the map and reduce classes
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(Reducer.class);

		// Set the output key and value class
		job.setOutputKeyClass(Key.class);
		job.setOutputValueClass(Value.class);

		// TODO Set the InputFormat and OutputFormat for the job

		// TODO Set the job to use the KeyRangePartitioner

		// TODO Configure the ZooKeeper instance and the Accumulo connection
		// objects
		ZooKeeperInstance instance = null;
		Connector connector = null;

		// TODO Delete the table if it already exists
		if (...) {
			
		}

		// TODO Get 10 split points from the FileSampling utility with a .01
		// sample rate
		SortedSet<Text> splitPoints = null;

		// Log the splits to stdout and write them to a splits file in HDFS
		PrintWriter out = null;

		for (...) {
			// hint: use Base64.encodeBase64
		}

		// TODO Close the HDFS file
		out.close();

		// Create the table and add the split points
		connector.tableOperations().create(Constants.TWITTER_TABLE);
		connector.tableOperations().addSplits(Constants.TWITTER_TABLE,
				splitPoints);

		// TODO Set the number of reducers based on the number of splits + 1
		job.setNumReduceTasks(splitPoints.size() + 1);

		// TODO Set the range partitioner's split file
		KeyRangePartitioner.setSplitFile(job, splitsFile.toString());

		// Run the MapReduce job
		int code = job.waitForCompletion(true) ? 0 : 1;

		// if the job was successful
		if (code == 0) {''
			// Open the permissions so the accumulo user can access the files
			fs.setPermission(rfileOutput, new FsPermission(FsAction.ALL,
					FsAction.ALL, FsAction.ALL));

			// Create a failures directory in HDFS and set open permissions
			fs.mkdirs(failures);
			fs.setPermission(failures, new FsPermission(FsAction.ALL,
					FsAction.ALL, FsAction.ALL));

			// TODO Import the raw RFiles into Accumulo
		}
		return code;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(true),
				new BulkTweetIngester(), args));
	}
}
