package com.clearedgeit.accumulo.examples;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import util.LocalEnvUtil;

public class IndexedDocReader {
  private static boolean USE_MINI_ACCUMULO = true;
  private static String userName = "root"; // The name of the Accumulo user
  private static String userPass = "trainingUser"; // The users password
  private static String twitterDataTable = "TwitterData"; // The table
                                                          // containing twitter
                                                          // information
  private static String tweetDocIndex = "TweetDocIndex"; // The table containing
                                                         // document partitioned
  private static String instanceName = "localAccumulo"; // The Accumulo instance
                                                        // name (stored in
                                                        // ZooKeeper)
  private static String zookeepers = "localhost"; // A comma separated list of
                                                  // ZooKeeper servers

  public static void main(String args[]) throws Exception {

    Connector connector;
    
    if(USE_MINI_ACCUMULO){
      connector = LocalEnvUtil.getConnector(userPass);
    } else {
      // Configure the ZooKeeper instance and the Accumulo connection objects
      ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zookeepers);
      connector = instance.getConnector(userName, new PasswordToken(
          userPass.getBytes()));
    }

    // Ensure the table exists
    if (!connector.tableOperations().exists(tweetDocIndex)) {
      System.out.format("Error: Table %s does not exist", tweetDocIndex);
      System.exit(-1);
    }

    // Manually add the terms to search for (a minimum of 2 must be specified)
    Text[] terms = { new Text("actually"), new Text("makes") };

    // Create a batch scanner to connect to the document index table
    BatchScanner batchScan = connector.createBatchScanner(tweetDocIndex,
        new Authorizations(), 1);

    // Create an IteratorSetting object to specify which
    IteratorSetting cfg = new IteratorSetting(20, "ii",
        IntersectingIterator.class.getName());

    // Configure the IntersectingIterator, by setting the column families to
    // search for
    IntersectingIterator.setColumnFamilies(cfg, terms);

    // Add the iterator as a scan time iterator
    batchScan.addScanIterator(cfg);

    // Configure the batch scanner to look at the entire table
    batchScan.setRanges(Collections.singleton(new Range()));

    // A list to hold all of the tweet IDs
    LinkedList<Range> tweetIds = new LinkedList<Range>();
    String tweetId;

    // Process all of the records returned by the batch scanner
    for (Entry<Key, Value> record : batchScan) {

      // Get the tweet ID and add it to the list of IDs
      tweetId = record.getKey().getColumnQualifier().toString();
      tweetIds.add(new Range(tweetId));
    }

    // Close the batch scanner
    batchScan.close();

    // Initialize the batch scanner to scan the twitter data table with
    // the previously found tweet IDs
    batchScan = connector.createBatchScanner(twitterDataTable,
        new Authorizations(), 10);
    batchScan.setRanges(tweetIds);

    String tweetText = null; // The tweet Text
    String userName = null; // The username of a tweet
    String colFam = null; // The Column Family of the current record

    // Process all of the records returned by the batch scanner
    for (Entry<Key, Value> record : batchScan) {

      // Get the column Family from the record's key
      colFam = record.getKey().getColumnFamily().toString();

      // Get the username from the row
      if (colFam.equals("uid")) {
        userName = record.getValue().toString();
      }

      // Get the tweet text from the row
      if (colFam.equals("text")) {
        tweetText = record.getValue().toString();
      }

      // Display the username and their corresponding tweet
      if (userName != null && tweetText != null) {
        System.out.format("%s:\t%s\n", userName, tweetText);

        // Reset the variables
        userName = null;
        tweetText = null;
      }
    }

    // Close the batch scanner
    batchScan.close();
  }
}