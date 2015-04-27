package de.tuberlin.dima.partitioning;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by mustafa on 27/04/15.
 */
public class MutableDataSetTextOutputFormat<T> extends TextOutputFormat {

    public MutableDataSetTextOutputFormat(Path outputPath) {
        super(outputPath, "UTF-8");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        editPathPerNode(taskNumber);
        System.out.println("This is my ID "+taskNumber+"***********************");
    }

    public void editPathPerNode(int taskNumber){
        this.setOutputFilePath(new Path(getOutputFilePath()+"/storage/"+taskNumber));
    }
}
