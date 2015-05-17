package de.tuberlin.dima.partitioning;

import de.tuberlin.dima.Person;
import de.tuberlin.dima.StudentInfo;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class MutuableDatasetPartitioning {


	private static final String TASK_INFO_ACCUMULATOR = "placementMap";

	public static void main(String[] args) {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);
		env.getConfig().setExecutionMode(ExecutionMode.BATCH_FORCED);


		// read the person data
		DataSet<Person> inPerson = env
				.readTextFile("../MutuableDatasetDemo/src/main/resources/Person.csv")
				.map(new PersonMapper());

		// read the student data
		DataSet<StudentInfo> inStudent = env
				.readTextFile("../MutuableDatasetDemo/src/main/resources/StudentInfo.csv")
				.map(new StudentInfoMapper());


		try {

			inPerson.partitionByHash("name") // TODO: as KeySelector
					.map(new TrackHost())
					.coGroup(inStudent.partitionByHash("name"))
					.where("name").equalTo("name")
					.with(new ComputeStudiesProfile())
					.write(new TextOutputFormat(new Path()), "/home/mustafa/Documents/tst/", FileSystem.WriteMode.OVERWRITE);



			final JobExecutionResult result = env.execute("Accumulator example");
			final List<Tuple2<Integer, String>> taskFields = result.getAccumulatorResult(TASK_INFO_ACCUMULATOR);

			System.out.format("number of objects in the map =  %s\n", taskFields);

		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
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

	public static class TrackHost<T> extends RichMapFunction<T, T> {

		private TaskAssignmentAccumulator taskAssignmentAccumulator = new TaskAssignmentAccumulator();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			// register the accumulator instance
			getRuntimeContext().addAccumulator(TASK_INFO_ACCUMULATOR, this.taskAssignmentAccumulator);

		}

		@Override
		public T map(Object value) throws Exception {

			// adding the host info to the local accumulator
			Tuple2<Integer, String> hostInfo = new Tuple2<Integer, String>(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getTaskName());
			//System.out.println("**************************" + hostInfo.f0 + "*************" + ((Person) value).getName());
			this.taskAssignmentAccumulator.add(hostInfo);
			return (T) value;
		}
	}

	public static class PersonMapper implements MapFunction<String, Person> {

		private Person person;

		@Override
		public Person map(String s) throws Exception {

			person = new Person();

			String[] splits = s.split(";");
			person.setName(splits[0]);
			person.setSchool(splits[1]);
			person.setSex((splits[2]).charAt(0));
			person.setAge(Integer.parseInt(splits[3]));

			return person;
		}
	}

	public static class StudentInfoMapper implements MapFunction<String, StudentInfo> {

		private StudentInfo studentInfo;

		@Override
		public StudentInfo map(String s) throws Exception {

			studentInfo = new StudentInfo();

			String[] split = s.split(";");
			studentInfo.setName(split[0]);
			studentInfo.setMajor(split[1]);

			String[] courses = split[2].split(",");

			for (String str : courses)
				studentInfo.getCourses().add(str);

			return studentInfo;
		}
	}

	public static class ComputeStudiesProfile implements CoGroupFunction<Person, StudentInfo, Person> {

		@Override
		public void coGroup(Iterable<Person> iterable, Iterable<StudentInfo> iterable1, Collector<Person> collector) throws Exception {

			Person person = iterable.iterator().next();

			ArrayList<StudentInfo> infos = new ArrayList<StudentInfo>();
			for (StudentInfo info : iterable1) {
				infos.add(info);
			}
			if (infos.size() > 0) {
				update(person, infos, collector);
			}
		}

		public void update(Person person, Collection<StudentInfo> infos, Collector<Person> collector) {
			person.setMajor(infos.iterator().next().getMajor());
			for(StudentInfo info : infos){
				person.getBestCourse().addAll(info.getCourses());
			}
			collector.collect(person);
		}
	}
}
