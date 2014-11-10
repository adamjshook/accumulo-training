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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.clearedgeit.accumulo.examples.util.Constants;
import com.clearedgeit.accumulo.examples.util.GeoUtil;

public class GeoSearcher extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		// Ensure the user enters a word to search for
		if (args.length != 4) {
			System.err
					.println("Usage: hadoop jar accumulo-geo-searcher.jar <lower_left_lat> "
							+ "<lower_left_lon> <upper_right_lat> <upper_right_lon>");
			return 1;
		}

		// Grab the command line arguments
		double llLat = Double.parseDouble(args[0]);
		double llLon = Double.parseDouble(args[1]);
		double urLat = Double.parseDouble(args[2]);
		double urLon = Double.parseDouble(args[3]);

		// Configure the ZooKeeper instance and the Connector objects
		ZooKeeperInstance instance = new ZooKeeperInstance(Constants.INSTANCE,
				Constants.ZOOKEEPERS);
		Connector connector = instance.getConnector(Constants.USER_NAME,
				new PasswordToken(Constants.USER_PASS));

		// Ensure both the twitter table and location index table exist
		if (!connector.tableOperations().exists(Constants.TWITTER_TABLE)
				|| !connector.tableOperations().exists(
						Constants.TWEET_LOCATION_INDEX_TABLE)) {
			System.out.format("Error: Table %s or table %s does not exist",
					Constants.TWITTER_TABLE,
					Constants.TWEET_LOCATION_INDEX_TABLE);
			return 1;
		}

		// Create a batch scanner against the tweet location index table
		BatchScanner batchScan = connector.createBatchScanner(
				Constants.TWEET_LOCATION_INDEX_TABLE, new Authorizations(), 10);

		// Encode the bounding box latitude and longitudes using the GeoUtil
		String encStartLocation = GeoUtil.encodeLocation(args[0], args[1]);
		String encEndLocation = GeoUtil.encodeLocation(args[2], args[3]);

		// Create a list of search Ranges, adding a Range containing the encoded
		// locations
		LinkedList<Range> searchRanges = new LinkedList<Range>();
		Range searchRange = new Range(encStartLocation, encEndLocation);
		searchRanges.add(new Range(searchRange));

		// Set the search ranges for our scanner
		batchScan.setRanges(searchRanges);

		// A list to hold all of the tweet IDs
		LinkedList<Range> tweetIds = new LinkedList<Range>();

		// A long to keep track of incorrect lookups
		long incorrectLookups = 0;

		// Process all of the records returned by the batch scanner
		for (Entry<Key, Value> record : batchScan) {
			// Decode the location using the GeoUtil
			double[] location = GeoUtil.decodeLocation(record.getKey().getRow()
					.toString());

			// Remove any keys outside the bounding box
			if (location[0] >= llLat && location[0] <= urLat
					&& location[1] >= llLon && location[1] <= urLon) {

				// Add the tweet ID to the list of IDs
				String tweetId = record.getKey().getColumnQualifier()
						.toString();
				tweetIds.add(new Range(tweetId));
			} else {
				++incorrectLookups;
			}
		}

		// Close the batch scanner
		batchScan.close();

		if (tweetIds.isEmpty()) {
			System.out
					.println("Found no tweets that matches the given bounding box");
			return 0;
		}

		// Initialize the batch scanner to scan the twitter data table with
		// the previously found tweet IDs as the ranges
		batchScan = connector.createBatchScanner(Constants.TWITTER_TABLE,
				new Authorizations(), 10);
		batchScan.setRanges(tweetIds);

		String text = null;
		String lat = null;
		String lon = null;
		String screenname = null;
		String colFam = null;
		String colQual = null;

		// Process all of the records returned by the batch scanner
		for (Entry<Key, Value> record : batchScan) {

			// Get the column family and qualifier from the record's key
			colFam = record.getKey().getColumnFamily().toString();
			colQual = record.getKey().getColumnQualifier().toString();

			// Get the screen name from the row
			if (colFam.equals("user") && colQual.equals("screenname")) {
				screenname = record.getValue().toString();
			}

			// Get the tweet text from the row
			if (colFam.equals("tweet") && colQual.equals("text")) {
				text = record.getValue().toString();
			}

			// Get the tweet latitude from the row
			if (colFam.equals("tweet") && colQual.equals("lat")) {
				lat = record.getValue().toString();
			}
			// Get the tweet longitude from the row
			if (colFam.equals("tweet") && colQual.equals("lon")) {
				lon = record.getValue().toString();
			}

			// Print the screen name and their corresponding tweet to stdout
			if (screenname != null) {
				System.out.format("%s:\t%s\t%s\t%s\n", screenname, lat, lon,
						text);

				// Reset the variables
				screenname = null;
			}
		}

		// Display some statistics about the lookup
		System.out.format("%s\nSearcher Statistics\n%s\n",
				StringUtils.repeat("-", 10), StringUtils.repeat("-", 10));
		System.out.format("Records read: %d\n", tweetIds.size()
				+ incorrectLookups);
		System.out.format("Records kept: %d\n", tweetIds.size());
		System.out.format("Records thrown out: %d\n", incorrectLookups);

		// Close the batch scanner
		batchScan.close();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner
				.run(new Configuration(), new GeoSearcher(), args));
	}
}
