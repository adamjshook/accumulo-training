package com.clearedgeit.accumulo.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.clearedgeit.accumulo.examples.util.Constants;

public class CustomIteratorReader extends Configured implements Tool {

	// Total number of rows read
	private long numRowsRead = 0;

	// Number of rows to display at a time
	private static final int NUM_ROWS_TO_DISPLAY = 10;

	@Override
	public int run(String[] arg0) throws Exception {
		// Create a buffered reader to read users input from the command line
		BufferedReader userResponse = new BufferedReader(new InputStreamReader(
				System.in));

		// TODO Configure the ZooKeeper instance and the Accumulo connection objects
		ZooKeeperInstance instance = null;
		Connector connector = null;

		// TODO Ensure the table exists
		if (...) {
			System.out.format("Error: Table %s does not exist",
					Constants.TWITTER_TABLE);
			return 1;
		}

		// TODO Create a ClientSideIteratorScanner to debug the iterator locally
		Scanner scanner = null;

		// TODO Configure the iterator and add it to the scanner
		IteratorSetting iterConfig = null;

		String tweetId;

		// TODO Process all of the records returned by the scanner
		for (...) {

			// increment the number of rows read
			++numRowsRead;

			// Limit the amount of rows being displayed
			if (numRowsRead % NUM_ROWS_TO_DISPLAY == 0) {
				// Ask the user if they want to see more entries, or exit
				System.out.format("%shit enter to continue or 'q' to quit%s",
						StringUtils.repeat("-", 10),
						StringUtils.repeat("-", 10));
				String response = userResponse.readLine();

				if (response.equals("q")) {
					System.exit(0);
				}
			}

			// TODO Get the tweet ID

			// TODO Write the tweet ID to stdout
		}

		// Display the number of rows read
		System.out.format("%d Entries read\n", numRowsRead);
		return 0;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new Configuration(),
				new CustomIteratorReader(), args));
	}
}