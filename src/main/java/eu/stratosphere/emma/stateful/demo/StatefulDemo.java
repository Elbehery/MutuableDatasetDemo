package eu.stratosphere.emma.stateful.demo;

import de.tuberlin.dima.flink.model.Person;
import de.tuberlin.dima.flink.model.StudentInfo;
import eu.stratosphere.emma.stateful.baseline.Stateful;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class StatefulDemo {

	public static void main(String[] args) {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		try {

			// read the person data
			DataSet<Person> people = env
					.readTextFile("../MutuableDatasetDemo/src/main/resources/Person.csv")
					.map(new MapFunction<String, Person>() {

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
					});

			DataSet<StudentInfo> inStudent = env
					.readTextFile("../MutuableDatasetDemo/src/main/resources/StudentInfo.csv")
					.map(new MapFunction<String, StudentInfo>() {

						private StudentInfo studentInfo;

						@Override
						public StudentInfo map(String value) throws Exception {
							studentInfo = new StudentInfo();

							String[] split = value.split(";");
							studentInfo.setName(split[0]);
							studentInfo.setMajor(split[1]);
							studentInfo.getCourses().add(split[2]);

							return studentInfo;
						}
					});



			// create a stateful dataset (forces execution)
			Stateful.Set<Person, String> model = new Stateful.Set<Person, String>(env, people,
					new KeySelector<Person, String>() {
						@Override
						public String getKey(Person person) throws Exception {
							return person.getName();
						}
					});


			DataSet<Person> tst = model.updateWith(new FlatMapFunction<Tuple2<Person, StudentInfo>, Person>() {

				@Override
				public void flatMap(Tuple2<Person, StudentInfo> value, Collector<Person> out) throws Exception {

					Person person = value.f0;
					StudentInfo studentInfo = value.f1;
					person.setMajor(studentInfo.getMajor());
					out.collect(person);

				}
			}, inStudent, new KeySelector<StudentInfo, String>() {
				@Override
				public String getKey(StudentInfo value) throws Exception {
					return value.getName();
				}
			});

			tst.print();
			System.out.println("DONE");

		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
