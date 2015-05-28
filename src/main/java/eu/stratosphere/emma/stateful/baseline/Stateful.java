package eu.stratosphere.emma.stateful.baseline;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.UUID;

public class Stateful {

    private static String STATEFUL_BASE_PATH = String.format("%s/emma/stateful", System.getProperty("java.io.tmpdir"));

    private static final String TASK_INFO_ACC_PREFIX = "stateful.map";

    /**
     * An abstraction for a stateful dataset which allows restricted point-wise updates.
     *
     * @param <A> Element type for the dataset.
     * @param <K> Key type dataset elements.
     */
    public static class DataSet<A, K extends Comparable<K>> {

        private final UUID uuid = UUID.randomUUID();

        /**
         * Convert the stateless dataset into a stateful one.
         *
         * @param stateless   The stateless dataset to be converted.
         * @param keySelector The key selector to be used for hashing.
         * @throws Exception
         */
        public DataSet(org.apache.flink.api.java.DataSet<A> stateless, KeySelector<A, K> keySelector) throws Exception {
            String outputPath = String.format("%s/%s", STATEFUL_BASE_PATH, uuid);
            stateless
                    .partitionByHash(keySelector)
                    .map(new HostTrackingMapper<A>(uuid))
                    .write(new TypeSerializerOutputFormat<A>(), outputPath, FileSystem.WriteMode.NO_OVERWRITE);
        }
    }

    private static class HostTrackingMapper<A> extends RichMapFunction<A, A> {

        private UUID uuid;

        public HostTrackingMapper(UUID uuid) {
            this.uuid = uuid;
        }

        private TaskAssignmentAccumulator taskAssignmentAccumulator = new TaskAssignmentAccumulator();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // register the accumulator instance
            getRuntimeContext().addAccumulator(String.format("%s.%s", TASK_INFO_ACC_PREFIX, uuid), this.taskAssignmentAccumulator);

        }

        @Override
        public A map(A value) throws Exception {
            // adding the host info to the local accumulator
            Tuple2<Integer, String> hostInfo = new Tuple2<Integer, String>(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getTaskName());
            this.taskAssignmentAccumulator.add(hostInfo);
            return value;
        }
    }


    // Accumulator to gather information about the task-manager, in order to materialize records of the same node in the same file
    public static class TaskAssignmentAccumulator implements Accumulator<Tuple2<Integer, String>, ArrayList<Tuple2<Integer, String>>> {

        private ArrayList<Tuple2<Integer, String>> hostInfo;

        public TaskAssignmentAccumulator() {
            this.hostInfo = new ArrayList<Tuple2<Integer, String>>();
        }

        @Override
        public void add(Tuple2<Integer, String> value) {
            this.hostInfo.add(value);
        }

        @Override
        public ArrayList<Tuple2<Integer, String>> getLocalValue() {
            return this.hostInfo;
        }

        @Override
        public void resetLocal() {
            this.hostInfo.clear();
        }

        @Override
        public void merge(Accumulator<Tuple2<Integer, String>, ArrayList<Tuple2<Integer, String>>> other) {
            this.hostInfo.addAll(other.getLocalValue());
        }

        @Override
        public Accumulator<Tuple2<Integer, String>, ArrayList<Tuple2<Integer, String>>> clone() {

            TaskAssignmentAccumulator clonedTaskAssignmentAccumulator = new TaskAssignmentAccumulator();
            clonedTaskAssignmentAccumulator.hostInfo = this.hostInfo;
            return clonedTaskAssignmentAccumulator;
        }
    }
}