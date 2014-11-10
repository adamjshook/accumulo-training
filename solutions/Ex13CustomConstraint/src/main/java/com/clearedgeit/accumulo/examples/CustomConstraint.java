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
		// Return the error description associated with the violation code
		switch (violationCode) {
		case MAX_VALUE_EXCEEDED:
			return maxValueExceeded;
		}

		// If no error description exists, return null
		return null;
	}

	/**
	 * Ensure mutations pass constraints
	 */
	public List<Short> check(Environment env, Mutation mutation) {

		// Create a list to hold violation codes
		ArrayList<Short> violations = new ArrayList<Short>();

		// Get the updates from the mutation object
		Collection<ColumnUpdate> updates = mutation.getUpdates();

		// Iterate through each record determining if a record passes
		// constraints
		for (ColumnUpdate columnUpdate : updates) {
			// Ignore any record that contains a delete
			if (!columnUpdate.isDeleted()) {
				// If the record is greater than the maximum value, add a
				// violation code to the list of violations
				if (Long.parseLong(new String(columnUpdate.getValue())) >= maxValue) {
					violations.add(MAX_VALUE_EXCEEDED);
				}
			}
		}

		// Return the list of violation codes
		return violations;
	}
}
