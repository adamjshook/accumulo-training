package com.clearedgeit.accumulo.examples;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;

public class CustomConstraint implements Constraint {

	// Violation codes
	private final static short MAX_VALUE_EXCEEDED = 0;

	// Violation string
	private final static String maxValueExceeded = "Maximum value exceeded";

	// Maximum value allowed
	private Long maxValue = (long) 100;

	/**
	 * Return the error description associated with a violation code
	 */
	public String getViolationDescription(short violationCode) {
		// TODO Return the error description associated with the violation code
		switch (...) {
		
		}

		// TODO If no error description exists, return null
	}

	/**
	 * Ensure mutations pass constraints
	 */
	public List<Short> check(Environment env, Mutation mutation) {

		// TODO Create a list to hold violation codes
		List<Short> violations = null;

		// TODO Get the updates from the mutation object
		Collection<ColumnUpdate> updates = null;

		// TODO Iterate through each record determining if a record passes
		// constraints
		for (...) {
			// TODO Ignore any record that contains a delete
			if (...) {
				// TODO If the record is greater than the maximum value, add a
				// violation code to the list of violations
				if (...) {
					
				}
			}
		}

		// TODO Return the list of violation codes
		
	}
}
