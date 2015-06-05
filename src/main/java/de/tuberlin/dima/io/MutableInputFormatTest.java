package de.tuberlin.dima.io;

import org.apache.flink.api.common.io.StrictlyLocalAssignment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.LocatableInputSplit;

import java.io.IOException;

/**
 * Created by mustafa on 27/05/15.
 */
public class MutableInputFormatTest extends TextInputFormat implements StrictlyLocalAssignment {


	public MutableInputFormatTest(Path filePath) {
		super(filePath);
	}


}
