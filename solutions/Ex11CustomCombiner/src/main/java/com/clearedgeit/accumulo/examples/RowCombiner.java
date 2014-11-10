package com.clearedgeit.accumulo.examples;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;

public class RowCombiner extends Combiner {

	@Override
	public Value reduce(Key key, Iterator<Value> iter) {
		// Set the sum of values to zero
		long sum = 0;

		// Loop through the key's values
		while (iter.hasNext()) {
			// Perform a safe add between the longs (hint: LongCombiner class)
			long value = Long.valueOf(iter.next().toString());
			sum = LongCombiner.safeAdd(sum, value);
		}

		// Return the value to the next iterator
		return new Value(Long.toString(sum).getBytes());
	}
}