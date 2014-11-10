package com.clearedgeit.accumulo.examples;

import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.clearedgeit.accumulo.examples.util.Constants;

public class TweetSearcher extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// Ensure the user enters a word to search for
		if (args.length < 1) {
			System.out
					.println("Usage: accumulo-searcher-example.jar <space-delimited words to search for>");
			return 1;
		}

		// Configure the ZooKeeper instance and the Accumulo connection
		// objects
		ZooKeeperInstance instance = new ZooKeeperInstance(Constants.INSTANCE,
				Constants.ZOOKEEPERS);
		Connector connector = instance.getConnector(Constants.USER_NAME,
				new PasswordToken(Constants.USER_PASS));

		// Ensure both tables exists
		if (!connector.tableOperations().exists(Constants.TWITTER_TABLE)) {
			System.out.format("Error: Table %s does not exist\n",
					Constants.TWITTER_TABLE);
			return 1;
		}

		if (!connector.tableOperations().exists(Constants.TWEET_INDEX_TABLE)) {
			System.out.format("Error: Table %s does not exist\n",
					Constants.TWEET_INDEX_TABLE);
			return 1;
		}

		long start = System.currentTimeMillis();

		// Create a scanner
		BatchScanner batchScan = connector.createBatchScanner(
				Constants.TWEET_INDEX_TABLE, new Authorizations(), 10);

		LinkedList<Range> searchRanges = new LinkedList<Range>();

		// Create a search Range from the command line args
		for (String searchTerm : args) {
			searchRanges.add(new Range(searchTerm));
		}

		// Set the search ranges for our scanner
		batchScan.setRanges(searchRanges);

		// A list to hold all of the tweet IDs
		LinkedList<Range> tweetIds = new LinkedList<Range>();
		String tweetId;

		// Process all of the records returned by the batch scanner
		for (Entry<Key, Value> record : batchScan) {
			// Get the tweet ID and add it to the list of tweet IDs
			tweetId = record.getKey().getColumnQualifier().toString();
			tweetIds.add(new Range(tweetId));
		}

		// Close the batch scanner
		batchScan.close();

		// If tweet IDs is empty, log a message and return 0
		if (tweetIds.isEmpty()) {
			System.out.println("Found no tweets with provided search terms");
			return 0;
		}

		// Initialize the batch scanner to scan the twitter data table with
		// the previously found tweet IDs as the ranges
		batchScan = connector.createBatchScanner(Constants.TWITTER_TABLE,
				new Authorizations(), 10);
		batchScan.setRanges(tweetIds);

		String tweetText = null; // The tweet Text
		String screenName = null; // The screen name of the tweeter
		String colFam = null; // The column family of the current record
		String colQual = null; // The column qualifier of the current record

		int numTweets = 0;
		// Process all of the records returned by the batch scanner
		for (Entry<Key, Value> record : batchScan) {

			// Get the column family and qualifier from the record's key
			colFam = record.getKey().getColumnFamily().toString();
			colQual = record.getKey().getColumnQualifier().toString();

			// If CF is "user" and CQ is "screenname", save the screen name
			if (colFam.equals("user") && colQual.equals("screenname")) {
				screenName = record.getValue().toString();
			}

			// If CF is "tweet" and CQ is "text", save the text
			if (colFam.equals("tweet") && colQual.equals("text")) {
				tweetText = record.getValue().toString();
			}

			// If screenname and tweet text are not null
			if (screenName != null && tweetText != null) {
				++numTweets;
				// Write the screen name and text to stdout
				System.out.format("%s:\t%s\n", screenName, tweetText);

				// Reset the variables
				screenName = null;
				tweetText = null;
			}
		}

		// Close the batch scanner
		batchScan.close();

		long finish = System.currentTimeMillis();

		System.out.format("Found %d tweets in %s ms\n", numTweets,
				(finish - start));
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new TweetSearcher(),
				args));
	}
}
