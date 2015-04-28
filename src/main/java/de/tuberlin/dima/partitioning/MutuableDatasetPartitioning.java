package de.tuberlin.dima.partitioning;

import de.tuberlin.dima.Person;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import java.util.ArrayList;
import java.util.HashMap;


public class MutuableDatasetPartitioning {

    private static final String TASK_INFO_ACCUMULATOR = "placementMap";


    public static void main(String[] args) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Person> in = env
            .readTextFile("../MutuableDatasetDemo/src/main/resources/Person.csv")
            .map(new PersonMapper());

        try {
            // convert operator starts here
            env.setParallelism(3);
            in.partitionByHash("name") // TODO: as KeySelector
                    .map(new TrackHostPerson())
                    .write(new TypeSerializerOutputFormat(), "/home/mustafa/Documents/tst/", FileSystem.WriteMode.OVERWRITE);

            //env.getExecutionPlan();
           final JobExecutionResult result = env.execute();
            HashMap<Integer, String> placementMap = result.getAccumulatorResult(TASK_INFO_ACCUMULATOR);
            System.out.format("number of objects in the map =  %s\n", placementMap);

            // placementMap[0] = "host-where-slot-1-was-scheduled"
            // placementMap[1] = "host-where-slot-2-was-scheduled"
            // placementMap[2] = "host-where-slot-3-was-scheduled"
            // convert operator ends here

        }catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }


    public static class TrackHostPerson<T> extends RichMapFunction<T,T> {

        private final TaskAssignmentAccumulator taskAssignmentAccumulator = new TaskAssignmentAccumulator();;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // adding the host info to the local accumulator
            Tuple2<Integer,String> hostInfo = new Tuple2<Integer,String>(getRuntimeContext().getIndexOfThisSubtask(),getRuntimeContext().getTaskName());
            this.taskAssignmentAccumulator.add(hostInfo);
            getRuntimeContext().addAccumulator(TASK_INFO_ACCUMULATOR, this.taskAssignmentAccumulator);
        }

        @Override
        public T map(T t) throws Exception {
            return t;
        }
    }

    public static  class TaskAssignmentAccumulator implements Accumulator<Tuple2<Integer, String>, ArrayList<Tuple2<Integer, String>>> {

        private ArrayList<Tuple2<Integer, String>> hostInfo;

        public TaskAssignmentAccumulator(){
            hostInfo = new ArrayList<Tuple2<Integer, String>>();
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


    public static class PersonMapper implements MapFunction<String, Person> {

        Person person;

        @Override
        public Person map(String s) throws Exception {

            person = new Person();

            String [] splits = s.split(";");
            person.setName(splits[0]);
            person.setSchool(splits[1]);
            person.setSex((splits[2]).charAt(0));
            person.setAge(Integer.parseInt(splits[3]));

            return person;
        }
    }

}
