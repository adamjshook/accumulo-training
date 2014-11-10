package com.clearedgeit.accumulo.examples.util;

public class Constants {
	// Accumulo Configuration
	public static final String USER_NAME = "root";
	public static final String USER_PASS = "secret";
	public static final String INSTANCE = "localAccumulo";
	public static final String ZOOKEEPERS = "localhost:2181";

	// Table names
	public static final String TWITTER_TABLE = "TwitterData";
	public static final String TWEET_INDEX_TABLE = "TweetIndex";
	public static final String TWEET_LOCATION_INDEX_TABLE = "TweetLocationIndex";
}
