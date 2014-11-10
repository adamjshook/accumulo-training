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
		// TODO Call the parent iterator's init

		// TODO Attempt to set the maximum value from the options
		if (...) {
		} else {
			// TODO Else, set a default
		}

		// TODO Attempt to set the minimum value from the options
		if (...) {
		} else {
			// TODO Else, set a default
		}
	}

	// Override deepCopy to copy the iterator and set the new instance with the
	// min and max value
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {

		// TODO Create a new CusomFilter object based on the parent's deepCopy
		CustomFilter copy = null;

		// TODO Replicate all of the current iterators options in the new
		// CustomFilter copy

		// TODO Return the CustomFilter copy
	}

	@Override
	public boolean accept(Key k, Value v) {
		// TODO If the current value is less than maxValue and greater than minValue
		// return true
		if (...) {
			
		}

		// TODO If the current value is not within range, return false
	}

	@Override
	public IteratorOptions describeOptions() {

		// TODO Get the parent iterator's iterator options
		IteratorOptions iterOpts = null;

		// TODO Set the iterators description

		// TODO Add options to allow min and max value to be specified
		
		// TODO Return the iterator's options
		return iterOpts;
	}

	// If this method is omitted you must configure default values in init
	@Override
	public boolean validateOptions(Map<String, String> options) {
		// TODO Call the parent iterator's validateOptions

		try {
			// TODO If the options contain a max value, ensure it is a long
			if (...) {
				
			}

			// TODO If the options contain a min value, ensure it is a long
			if (...) {
				
			}
		} catch (Exception e) {
			// TODO If an exception occurred with converting the strings to long,
			// return false
		}

		// TODO If all of the options were successfully validated, return true
		
	}
}