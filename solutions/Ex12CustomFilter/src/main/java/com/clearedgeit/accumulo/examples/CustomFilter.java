package com.clearedgeit.accumulo.examples;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class CustomFilter extends Filter {

	// Available Options
	private static final String MAX_VALUE = "maxValue";
	private static final String MIN_VALUE = "minValue";

	// Private variables to hold the minVale and maxValues
	private long minValue;
	private long maxValue;

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		// Call the parent iterator's init
		super.init(source, options, env);

		// Attempt to set the minimum value from the options
		if (options.containsKey(MAX_VALUE)) {
			maxValue = Long.parseLong(options.get(MAX_VALUE));
		} else {
			// Else, set a default
			maxValue = Long.MAX_VALUE;
		}

		// Attempt to set the minimum value from the options
		if (options.containsKey(MIN_VALUE)) {
			minValue = Long.parseLong(options.get(MIN_VALUE));
		} else {
			// Else, set a default
			minValue = Long.MIN_VALUE;
		}
	}

	// Override deepCopy to copy the iterator and set the new instance with the
	// min and max value
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {

		// Create a new CusomFilter object based on the parent's deepCopy
		CustomFilter copy = (CustomFilter) super.deepCopy(env);

		// Replicate all of the current iterators options in the new
		// CustomFilter copy
		copy.maxValue = this.maxValue;
		copy.minValue = this.minValue;

		// Return the CustomFilter copy
		return copy;
	}

	@Override
	public boolean accept(Key k, Value v) {
		// If the current value is less than maxValue and greater than minValue
		// return true
		if (Long.valueOf(v.toString()) < this.maxValue
				&& Long.valueOf(v.toString()) > this.minValue) {
			return true;
		}

		// If the current value is not within range, return false
		return false;
	}

	@Override
	public IteratorOptions describeOptions() {

		// Get the parent iterator's iterator options
		IteratorOptions iterOpts = super.describeOptions();

		// Set the iterators description
		iterOpts.setDescription("The LocationFilter allows you to "
				+ "filter key/value pairs based on locations");

		// Add options to allow min and max value to be specified
		iterOpts.addNamedOption(CustomFilter.MAX_VALUE,
				"maximum value allowed (default is Long.MAX_VALUE)");
		iterOpts.addNamedOption(CustomFilter.MIN_VALUE,
				"minimum value allowed (default is Long.MIN_VALUE)");

		// Return the iterator's options
		return iterOpts;
	}

	// If this method is omitted you must configure default values in init
	@Override
	public boolean validateOptions(Map<String, String> options) {
		// Call the parent iterator's validateOptions
		super.validateOptions(options);

		try {
			// If the options contain a max value, ensure it is a long
			if (options.containsKey(MAX_VALUE)) {
				Long.parseLong(options.get(MAX_VALUE));
			}

			// If the options contain a min value, ensure it is a long
			if (options.containsKey(MIN_VALUE)) {
				Long.parseLong(options.get(MIN_VALUE));
			}
		} catch (Exception e) {
			// If an exception occurred with converting the strings to long,
			// return false
			return false;
		}

		// If all of the options were successfully validated, return true
		return true;
	}
}