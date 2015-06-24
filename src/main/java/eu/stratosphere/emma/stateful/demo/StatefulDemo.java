package eu.stratosphere.emma.stateful.demo;

import de.tuberlin.dima.flink.model.Person;
import de.tuberlin.dima.flink.model.StudentInfo;
import eu.stratosphere.emma.stateful.baseline.Stateful;
import eu.stratosphere.emma.stateful.baseline.StatefulAdapter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import scala.util.Either;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


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


			//TODO: Trial of Adapter Pattern with Scala
			StatefulAdapter s = new StatefulAdapter(env, people, new KeySelector<Person,String>() {
				@Override
				public String getKey(Person value) throws Exception {
					return value.getName();
				}
			});

			/*// create a stateful dataset (forces execution)
			Stateful.Set<Person, String> model = new Stateful.Set<Person, String>(env, people,
					new KeySelector<Person, String>() {
						@Override
						public String getKey(Person person) throws Exception {
							return person.getName();
						}
					});
*/
			DataSet<Either<Person,Person>> tst = s.stateful().updateWith(new UpdateStudentMajor(), inStudent, new KeySelector<StudentInfo, String>() {
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

	private static class UpdateStudentMajor implements FlatMapFunction<Tuple2<Person, Collection<StudentInfo>>, Person>, ResultTypeQueryable<Person> {

		@Override
		public void flatMap(Tuple2<Person, Collection<StudentInfo>> value, Collector<Person> out) throws Exception {

			Person person = value.f0;
			List<StudentInfo> studentInfoList = new ArrayList<StudentInfo>(value.f1);

			for(StudentInfo studentInfo : studentInfoList){
				value.f0.setMajor(studentInfo.getMajor());
				value.f0.getCourses().addAll(studentInfo.getCourses());
			}

			out.collect(person);
		}

		@Override
		public TypeInformation<Person> getProducedType() {
			return new GenericTypeInfo<Person>(Person.class);
		}
	}
}
