package com.clearedgeit.accumulo.examples.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class FileSampling {

	// The number of tweets to hold in a partition
	public static final int SAMPLE_SIZE = 10000;

	public static class TweetIdSRSMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		private Status status = null;
		private Random rndm = new Random();
		public static final String SAMPLE_RATE = "mapreduce.tweetidsrsmapper.sample.rate";
		private double sampleRate = .05;
		private NullWritable outkey = NullWritable.get();
		private Text outvalue = new Text();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			sampleRate = context.getConfiguration().get(SAMPLE_RATE) != null ? Double
					.parseDouble(context.getConfiguration().get(SAMPLE_RATE))
					: sampleRate;
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (rndm.nextDouble() < sampleRate) {
				try {
					status = TwitterObjectFactory
							.createStatus(value.toString());
				} catch (TwitterException e) {
					return;
				}
				outvalue.set(Long.toString(status.getId()));
				context.write(outkey, outvalue);
			}
		}
	}

	public static class TweetIdSRSReducer extends
			Reducer<NullWritable, Text, NullWritable, Text> {

		public static final String NUM_SAMPLES = "mapreduce.tweetidsrsreducer.num.samples";
		private int numsamples = 10;
		private NullWritable outkey = NullWritable.get();
		private Text outvalue = new Text();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			numsamples = context.getConfiguration().get(NUM_SAMPLES) != null ? Integer
					.parseInt(context.getConfiguration().get(NUM_SAMPLES))
					: numsamples;
		}

		@Override
		protected void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			List<String> splits = new ArrayList<String>();
			for (Text t : values) {
				splits.add(t.toString());
			}

			if (splits.size() < numsamples) {
				throw new IOException(
						"Number of samples is less than the desired number, increase the sample rate");
			}

			Collections.sort(splits);

			int currIndex = 0;
			int toSkip = splits.size() / (numsamples + 1);
			for (int i = 0; i < numsamples; ++i) {
				currIndex += toSkip;
				outvalue.set(splits.get(currIndex));
				context.write(outkey, outvalue);
			}
		}
	}

	/**
	 * Execute a MapReduce job to sample the given JSON tweet input, returning a
	 * number of equi-distant splits
	 * 
	 * @param conf
	 *            A job Configuration
	 * @param inputPath
	 *            The JSON tweet input path
	 * @param workingDir
	 *            A working directory, which is deleted if successful
	 * @param sampleRate
	 *            The rate at which the keys are sampled
	 * @param numSplits
	 *            The number of input splits to return
	 * @return A set of equi-distant keys determined by the sampled input
	 * @throws Exception
	 *             If the job fails or some other error
	 */
	public static SortedSet<Text> getSplits(Configuration conf, Path inputPath,
			Path workingDir, double sampleRate, int numSplits) throws Exception {

		Job job = Job.getInstance(conf);
		job.setJarByClass(FileSampling.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job, inputPath);
		TextOutputFormat.setOutputPath(job, workingDir);
		job.getConfiguration().set("mapred.textoutputformat.separator", "");
		job.setMapperClass(TweetIdSRSMapper.class);
		job.getConfiguration().set(TweetIdSRSMapper.SAMPLE_RATE,
				Double.toString(sampleRate));
		job.getConfiguration().set(TweetIdSRSReducer.NUM_SAMPLES,
				Integer.toString(numSplits - 1));
		job.setReducerClass(TweetIdSRSReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		int code = job.waitForCompletion(true) ? 0 : 1;

		if (code == 0) {
			// Create an empty tree set
			SortedSet<Text> partitionKeys = new TreeSet<Text>();

			// Get a fs FileSystem Object
			FileSystem fs = FileSystem.get(conf);

			BufferedReader rdr = new BufferedReader(new InputStreamReader(
					fs.open(new Path(workingDir, "part-r-00000"))));

			String line;
			while ((line = rdr.readLine()) != null) {
				partitionKeys.add(new Text(line));
			}
			rdr.close();

			fs.delete(workingDir, true);
			// Return the partition keys
			return partitionKeys;
		} else {
			throw new Exception(
					"MR job failed to gather splits from input files");
		}
	}
}
