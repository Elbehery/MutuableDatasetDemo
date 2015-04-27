package de.tuberlin.dima.partitioning;

import de.tuberlin.dima.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;


import java.nio.file.Paths;


public class MutuableDatasetPartitioning {

    public static void main(String[] args) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataSet<String> personDataString = env.readTextFile("../MutuableDatasetDemo/src/main/resources/Person.csv");

        DataSet<Person> personDataSet = personDataString.map(new PersonMapper());


        // writing the dataset after partitioning with a custom outputFormat, trying to edit the output folder dynamically per each node
        personDataSet.partitionByHash("name").write(new MutableDataSetTextOutputFormat<Person>(new Path()),"/home/mustafa/Documents/tst/", FileSystem.WriteMode.OVERWRITE);

        try {

            // printing the execution plan to check the id of the node after re-partitioning
            System.out.println(personDataSet.partitionByHash("name").getExecutionEnvironment().getExecutionPlan());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            env.execute();
        }catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
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
