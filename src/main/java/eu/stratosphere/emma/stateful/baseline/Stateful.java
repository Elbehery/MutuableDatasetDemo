package eu.stratosphere.emma.stateful.baseline;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.event.task.IntegerTaskEvent;
import org.apache.flink.util.Collector;

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
    public static class Set<A, K extends Comparable<K>> {

        private final UUID uuid = UUID.randomUUID();
        private final String path = String.format("%s/%s", STATEFUL_BASE_PATH, uuid);
        private final ExecutionEnvironment env;
        private final KeySelector<A, K> statefulKey;
        private final ArrayList<Tuple2<Integer, String>> taskAssignmentMap;
        private final TypeInformation<A> typeInformation;

        /**
         * Convert the stateless dataset into a stateful one.
         *
         * @param stateless The stateless dataset to be converted.
         * @param key       The key selector to be used for hashing.
         * @throws Exception
         */
        public Set(ExecutionEnvironment env, DataSet<A> stateless, KeySelector<A, K> key) throws Exception {
            // create dataflow
            String taskAssignmentAccumulatorName = String.format("%s.%s", TASK_INFO_ACC_PREFIX, uuid);
            stateless
                    .partitionByHash(key)
                    .map(new HostTrackingMapper<A>(taskAssignmentAccumulatorName))
                    .write(new TypeSerializerOutputFormat<A>(), path, FileSystem.WriteMode.NO_OVERWRITE);

            // execute dataflow
            JobExecutionResult result = env.execute("Stateful[Create]");

            // fetch
            this.env = env;
            this.statefulKey = key;
            this.taskAssignmentMap = result.getAccumulatorResult(taskAssignmentAccumulatorName);
            this.typeInformation = stateless.getType();
        }

        public <B, C> DataSet<C> updateWith(FlatMapFunction<Tuple2<A, B>, C> udf, DataSet<B> updates, KeySelector<B, K> updateKey) {

            env.readFile(new TypeSerializerInputFormat<A>(typeInformation), path)
                    .coGroup(updates)
                    .where(statefulKey).equalTo(updateKey).with(new StatefulUpdater<A, B, Object>())


            return null;
        }
    }


    private static class StatefulUpdater<A, B, C> implements CoGroupFunction<A, B, C> {

        @Override
        public void coGroup(Iterable<A> first, Iterable<B> second, Collector<C> out) throws Exception {

        }

    }

    /**
     * A special mapper that tracks the placement of an execution vertex. Used just before the write task in order
     * to obtain a "taskID -> hostName" map for a stateful dataset.
     *
     * @param <A> Element type for the dataset.
     */
    private static class HostTrackingMapper<A> extends RichMapFunction<A, A> {

        private String taskAssignmentAccumulatorName;

        public HostTrackingMapper(String taskAssignmentAccumulatorName) {
            this.taskAssignmentAccumulatorName = taskAssignmentAccumulatorName;
        }

        private TaskAssignmentAccumulator taskAssignmentAccumulator = new TaskAssignmentAccumulator();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            RuntimeContext ctx = getRuntimeContext();

            // adding the host info to the local accumulator
            // FIXME: we need to get the proper hostname via the RuntimeContext. If this is not possible at the moment, open a PR. This is a blocker and needs to be resolved ASAP.
            Tuple2<Integer, String> hostInfo = new Tuple2<Integer, String>(getRuntimeContext().getIndexOfThisSubtask(), "localhost");
            this.taskAssignmentAccumulator.add(hostInfo);

            // register the accumulator instance
            getRuntimeContext().addAccumulator(taskAssignmentAccumulatorName, this.taskAssignmentAccumulator);

        }

        @Override
        public A map(A value) throws Exception {
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