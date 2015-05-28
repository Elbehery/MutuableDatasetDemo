package eu.stratosphere.emma.stateful.demo;

import de.tuberlin.dima.model.Person;
import eu.stratosphere.emma.stateful.baseline.Stateful;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;


public class StatefulDemo {

    public static void main(String[] args) {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        try {
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

            // read the person data
            Stateful.DataSet<Person, String> model = new Stateful.DataSet<Person, String>(
                    people,
                    new KeySelector<Person, String>() {
                        @Override
                        public String getKey(Person person) throws Exception {
                            return person.getName();
                        }
                    });

            env.execute("Stateful[Create]");

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

}
