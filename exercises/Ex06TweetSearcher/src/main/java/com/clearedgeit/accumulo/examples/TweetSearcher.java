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
		ZooKeeperInstance instance = null;
		Connector connector = null;

		// TODO Ensure both tables exists
		if (...) {
			System.out.format("Error: Table %s does not exist\n",
					Constants.TWITTER_TABLE);
			return 1;
		}

		if (...) {
			System.out.format("Error: Table %s does not exist\n",
					Constants.TWEET_INDEX_TABLE);
			return 1;
		}

		long start = System.currentTimeMillis();

		// TODO Create a scanner
		BatchScanner batchScan = null;

		LinkedList<Range> searchRanges = new LinkedList<Range>();

		// TODO Create a search Range from the command line args
		for (...) {
			
		}

		// TODO Set the search ranges for our scanner
		

		// A list to hold all of the tweet IDs
		LinkedList<Range> tweetIds = new LinkedList<Range>();
		String tweetId;

		// TODO Process all of the records returned by the batch scanner
		for (...) {
			// TODO Get the tweet ID and add it to the list of tweet IDs
			
		}

		// TODO Close the batch scanner
		
		// If tweet IDs is empty, log a message and return 0
		if (tweetIds.isEmpty()) {
			System.out.println("Found no tweets with provided search terms");
			return 0;
		}		

		// TODO Initialize the batch scanner to scan the twitter data table with
		// the previously found tweet IDs as the ranges
		batchScan = null;

		String tweetText = null; // The tweet Text
		String screenName = null; // The screen name of the tweeter
		String colFam = null; // The column family of the current record
		String colQual = null; // The column qualifier of the current record

		int numTweets = 0;
		// TODO Process all of the records returned by the batch scanner
		for (...) {

			// TODO Get the column family and qualifier from the record's key

			// TODO If CF is "user" and CQ is "screenname", save the screen name
			if (...) {
				
			}

			// TODO If CF is "tweet" and CQ is "text", save the text
			if (...) {
				
			}

			// TODO If screenname and tweet text are not null
			if (screenName != null && tweetText != null) {
				++numTweets;
				// TODO Write the screen name and text to stdout

				// TODO Reset the variables

			}
		}

		// TODO Close the batch scanner

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
