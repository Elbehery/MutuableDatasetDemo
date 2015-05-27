package de.tuberlin.dima.io;

import org.apache.flink.api.common.io.StrictlyLocalAssignment;
import org.apache.flink.api.java.io.TextInputFormat;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.core.io.LocatableInputSplit;


import java.io.IOException;
import java.util.Arrays;

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
