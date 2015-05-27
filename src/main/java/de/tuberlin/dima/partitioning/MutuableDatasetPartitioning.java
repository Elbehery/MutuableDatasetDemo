package de.tuberlin.dima.partitioning;

import de.tuberlin.dima.io.MutableInputFormat;
import de.tuberlin.dima.io.MutableInputFormatTest;
import de.tuberlin.dima.model.StudentJobs;
import de.tuberlin.dima.model.Person;
import de.tuberlin.dima.model.StudentInfo;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.LocatableInputSplit;
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

		DataSet<StudentJobs> inJobs = env
				.readTextFile("../MutuableDatasetDemo/src/main/resources/Jobs.csv")
				.map(new StudentJobsMapper());


		try {

			inPerson.partitionByHash("name") // TODO: as KeySelector
					.map(new TrackHost())
					.coGroup(inStudent.partitionByHash("name"))
					.where("name").equalTo("name")
					.with(new ComputeStudiesProfile())
					.write(new TextOutputFormat(new Path()), "file:///home/mustafa/Documents/tst/", FileSystem.WriteMode.OVERWRITE);

			//DataSet<Person> secondIn = env.readFile(new TypeSerializerInputFormat(new GenericTypeInfo(Person.class)),"/home/mustafa/Documents/tst/1");
			LocatableInputSplit [] splits = new LocatableInputSplit[env.getParallelism()];
			splits[0] = new LocatableInputSplit(env.getParallelism(),"localhost");
			splits[1] = new LocatableInputSplit(env.getParallelism(),"localhost");
			splits[2] = new LocatableInputSplit(env.getParallelism(),"localhost");
			DataSet<Person> secondIn = env.readFile(new MutableInputFormatTest(new Path(),splits),"file:///home/mustafa/Documents/tst/1").map(new PersonMapper());
			secondIn.print();

/*

			TypeSerializerInputFormat<Person> typeSerializerInputFormat = new TypeSerializerInputFormat(new GenericTypeInfo(Person.class));
			FileInputSplit [] writtenPerson = new FileInputSplit[env.getParallelism()];

			int iter = 0;
			for(FileInputSplit split : writtenPerson){
				split = new FileInputSplit(iter,new Path("/home/mustafa/Documents/tst/"),0,-1,null);
				typeSerializerInputFormat.open(split);
				iter++;
			}
			//typeSerializerInputFormat.getInputSplitAssigner()
*/


			//inJobs.write(new TextOutputFormat(new Path()), "/home/mustafa/Documents/tst/jobs/", FileSystem.WriteMode.OVERWRITE);

			final JobExecutionResult result = env.execute("Accumulator example");
			final List<Tuple2<Integer, String>> taskFields = result.getAccumulatorResult(TASK_INFO_ACCUMULATOR);

			//System.out.format("number of objects in the map =  %s\n", taskFields);

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

	public static class StudentJobsMapper implements MapFunction<String, StudentJobs> {

		private StudentJobs studentJobs;

		@Override
		public StudentJobs map(String s) throws Exception {

			studentJobs = new StudentJobs();

			String[] splits = s.split(";");
			studentJobs.setName(splits[0]);

			for(int x=1; x<splits.length; x++ ){
				studentJobs.getJobs().add(splits[x]);
			}
			return studentJobs;
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
