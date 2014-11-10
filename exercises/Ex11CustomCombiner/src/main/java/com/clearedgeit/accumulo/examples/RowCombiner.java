package com.clearedgeit.accumulo.examples;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;

public class RowCombiner extends Combiner {

	@Override
	public Value reduce(Key key, Iterator<Value> iter) {
		// TODO Set the sum of values to zero

		// TODO Loop through the key's values
		while (...) {
			// TODO Perform a safe add between the longs (hint: LongCombiner class)
		}

		// TODO Return the value to the next iterator
	}
}