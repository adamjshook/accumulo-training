package com.clearedgeit.accumulo.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.clearedgeit.accumulo.examples.util.Constants;

public class AccumuloReader extends Configured implements Tool {

	// Total number of rows read
	private static long numRowsRead = 0;

	// Number of rows to display at a time
	private static int numRowsDisplayed = 10;

	public int run(String[] args) throws Exception {

		// TODO Create ZooKeeperInstance and Connector objects
		ZooKeeperInstance zoo = null;
		Connector connector = null;

		// Create a buffered reader to read users input from the command line
		BufferedReader userResponse = new BufferedReader(new InputStreamReader(
				System.in));

		// TODO Ensure the table exists
		if (...) {
			System.err.format("Error: Table %s does not exist",
					Constants.TWITTER_TABLE);
			return 1;
		}

		// TODO Create a scanner to iterate over tweet records
		Scanner scanner = null;

		// TODO Limit the scanner to only fetch the text column from the tweet family
		

		String tweetId; // The tweet Id
		String tweetText; // The tweet Text

		// Process all of the records returned by the scanner
		for (Entry<Key, Value> record : scanner) {

			// TODO increment the number of rows read
			++numRowsRead;

			// Limit the amount of rows being displayed
			if (numRowsRead % numRowsDisplayed == 0) {
				System.out.format("%shit enter to continue or 'q' to quit%s",
						StringUtils.repeat("-", 10),
						StringUtils.repeat("-", 10));

				String response = userResponse.readLine();

				if (response.equals("q")) {
					break;
				}
			}

			// TODO Get the tweet ID and text value from the record

			// TODO Display the tweet ID and text
		}

		// Display the number of rows read
		System.out.format("%d Entries read\n", numRowsRead);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new AccumuloReader(),
				args));
	}
}