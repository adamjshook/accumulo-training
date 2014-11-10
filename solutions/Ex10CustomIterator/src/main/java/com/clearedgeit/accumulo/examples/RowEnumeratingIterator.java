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
		// Init is called by the framework and provides an initially scanner
		// object for us to leverage
		this.source = source;
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies,
			boolean inclusive) throws IOException {
		// Call the source iterator's seek method
		this.source.seek(range, columnFamilies, inclusive);

		// Find the next key/value pair to process (findTop)
		findTop();
	}

	@Override
	public void next() throws IOException {
		// Move the source to the next key/value pair
		this.source.next();

		// Find the next key/value pair to process (findTop)
		findTop();
	}

	private void findTop() throws IOException {
		// Continue to call the source iterator's next while the source hasTop,
		// the source top key is not a delete key, and the current key's rowID
		// != prevRowId
		while (this.source.hasTop() && !this.source.getTopKey().isDeleted()
				&& this.source.getTopKey().getRow().equals(prevRowId)) {
			this.source.next();
		}

		// Set our previous rowID to the source's top key rowID
		prevRowId = this.source.getTopKey().getRow();
	}

	@Override
	public Key getTopKey() {
		// Wrap the source's getTopKey
		return this.source.getTopKey();
	}

	@Override
	public Value getTopValue() {
		// Wrap the source's getTopValue
		return this.source.getTopValue();
	}

	@Override
	public boolean hasTop() {
		// Wrap the source's hasTop
		return this.source.hasTop();
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		throw new UnsupportedOperationException();
	}
}
