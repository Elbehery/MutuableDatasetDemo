package de.tuberlin.dima.flink.io;

import org.apache.flink.api.common.io.StrictlyLocalAssignment;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.core.io.LocatableInputSplit;


public class MutableInputFormat  implements InputSplitSource<LocatableInputSplit>,StrictlyLocalAssignment {



	@Override
	public LocatableInputSplit[] createInputSplits(int minNumSplits) throws Exception {
		return new LocatableInputSplit[0];
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(LocatableInputSplit[] inputSplits) {
		return null;
	}
}
