package com.clearedgeit.accumulo.examples;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class RowEnumeratingIterator implements
		SortedKeyValueIterator<Key, Value> {

	// The previous row's ID
	private Text prevRowId = null;

	// The source iterator
	private SortedKeyValueIterator<Key, Value> source = null;

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		// TODO Init is called by the framework and provides an initially
		// scanner
		// object for us to leverage
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies,
			boolean inclusive) throws IOException {
		// TODO Call the source iterator's seek method

		// TODO Find the next key/value pair to process (findTop)
	}

	@Override
	public void next() throws IOException {
		// TODO Move the source to the next key/value pair

		// TODO Find the next key/value pair to process (findTop)
	}

	private void findTop() {
		// TODO Continue to call the source iterators next while the source hasTop,
		// the source top key is not a delete key, and the current key's rowID
		// != prevRowId
		while (...) {

		}

		// TODO Set our previous rowID to the source's top key rowID
	}

	@Override
	public Key getTopKey() {
		// TODO Wrap the source's getTopKey
	}

	@Override
	public Value getTopValue() {
		// TODO Wrap the source's getTopValue
	}

	@Override
	public boolean hasTop() {
		// TODO Wrap the source's hasTop
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		throw new UnsupportedOperationException();
	}
}
