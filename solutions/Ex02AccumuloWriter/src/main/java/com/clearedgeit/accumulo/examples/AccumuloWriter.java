package com.clearedgeit.accumulo.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import com.clearedgeit.accumulo.examples.util.Constants;

public class AccumuloWriter extends Configured implements Tool {

	private static long numTweets = 0; // The total number of tweets inserted

	public int run(String args[]) throws Exception {

		// Ensure the user enters the path to the twitter data
		if (args.length != 1) {
			System.out
					.println("Usage: hadoop jar accumulo-writer-example.jar <data_dir>");
			return 1;
		}

		// Set the location of the twitter data
		String dataDir = args[0];

		// Get the names of all of the files containing twitter data
		File[] files = new File(dataDir).listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith(".json");
			}
		});

		// Configure the ZooKeeper instance and the Connector objects
		ZooKeeperInstance instance = new ZooKeeperInstance(
				Constants.INSTANCE.toString(), Constants.ZOOKEEPERS.toString());

		Connector connector = instance.getConnector(Constants.USER_NAME,
				new PasswordToken(Constants.USER_PASS.getBytes()));

		System.out.format("Checking for table %s's existance\n",
				Constants.TWITTER_TABLE);
		// Create our table if it does not already exist
		if (!connector.tableOperations().exists(Constants.TWITTER_TABLE)) {
			System.out.format("Creating table %s\n", Constants.TWITTER_TABLE);
			connector.tableOperations().create(Constants.TWITTER_TABLE);
		} else {
			// If it does, delete the table and create it again
			System.out.format("Deleting table %s\n", Constants.TWITTER_TABLE);
			connector.tableOperations().delete(Constants.TWITTER_TABLE);
			System.out.format("Creating table %s\n", Constants.TWITTER_TABLE);
			connector.tableOperations().create(Constants.TWITTER_TABLE);
		}

		// Set the BatchWriter configurations
		long memBuf = 1000000L; // bytes to store before sending a batch
		long timeout = 1000L; // Milliseconds to wait before sending
		int numThreads = 10; // Threads to use to write

		// Create the BatchWriter
		BatchWriterConfig batchConfig = new BatchWriterConfig();
		batchConfig.setTimeout(timeout, TimeUnit.MILLISECONDS);
		batchConfig.setMaxMemory(memBuf);
		batchConfig.setMaxWriteThreads(numThreads);
		BatchWriter batchWriter = connector.createBatchWriter(
				Constants.TWITTER_TABLE, batchConfig);

		// Read all of the files in the data directory
		for (File file : files) {
			System.out.format("Reading from file: %s\n\n", file);
			BufferedReader buffReader = null;

			// A JSON object holding tweet info
			String rawJson;

			// A record to be entered into the database
			Mutation mutation;

			// Create a buffered reader for the input file
			buffReader = new BufferedReader(new FileReader(file));

			// Read the file and insert tweet information into Accumulo
			while ((rawJson = buffReader.readLine()) != null) {
				Status tweet;
				Object o = TwitterObjectFactory.createObject(rawJson);

				if (o instanceof Status) {
					tweet = (Status) o;

					// Only process if the tweet is in English
					if (tweet.getUser().getLang().equals("en")) {
						// Create a new mutation with the tweet ID as the
						// row ID
						mutation = new Mutation(new Text(Long.toString(tweet
								.getId())));

						// (tweetId, "tweet", "text", text)
						mutation.put("tweet", "text", tweet.getText());

						// (tweetId, "tweet", "created_at", creation_date)
						mutation.put("tweet", "created_at", tweet
								.getCreatedAt().toString());

						// If geo location is not null
						if (tweet.getGeoLocation() != null) {
							// (tweetId, "tweet", "lat", geoLoc.lat)
							mutation.put("tweet", "lat", Double.toString(tweet
									.getGeoLocation().getLatitude()));

							// (tweetId, "tweet", "lon", geoLoc.lon)
							mutation.put("tweet", "lon", Double.toString(tweet
									.getGeoLocation().getLongitude()));
						}

						// (tweetId, "user", "id" user.id)
						mutation.put("user", "id",
								Long.toString(tweet.getUser().getId()));

						// (tweetId, "user", "screenname", user.screenName)
						mutation.put("user", "screenname", tweet.getUser()
								.getScreenName());

						// (tweetId, "user", "lang", language)
						mutation.put("user", "lang", tweet.getUser().getLang());

						// Add the mutation to the batch writer
						batchWriter.addMutation(mutation);

						// Increment the number of tweets inserted
						++numTweets;
					}
				}
			}
			buffReader.close();
		}

		// Send the mutation to Accumulo and release resources
		batchWriter.close();

		// Display how many tweets were inserted into Accumulo
		System.out.format("%d Tweets inserted\n", numTweets);
		return 0;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new AccumuloWriter(),
				args));
	}
}