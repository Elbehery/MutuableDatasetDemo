package de.tuberlin.dima;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Collection;


public class Main {

    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // env.getConfig().disableObjectReuse();

        DataSet<String> personDataString = env.readTextFile("../MutuableDatasetDemo/src/main/resources/Person.csv");
        DataSet<String> studentInfoDataSourceString = env.readTextFile("../MutuableDatasetDemo/src/main/resources/StudentInfo.csv");

        DataSet<Person> personDataSet = personDataString.map(new PersonMapper());
        DataSet<StudentInfo> studentInfoDataSet = studentInfoDataSourceString.map(new StudentInfoMapper());

        DataSet<Tuple3<String, String, String>> r1 = personDataSet.coGroup(studentInfoDataSet).where("name").equalTo("name").with(new ComputeStudiesProfile());
     //   DataSet<Tuple2<String, String>> r2 = personDataSet.coGroup(studentInfoDataSet).where("name").equalTo("name").with(new ComputeStudiesProfile());

        r1.print();
      //  r2.print();

      //  personDataSet.print();

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

    public static class StudentInfoMapper implements MapFunction<String, StudentInfo>{

         StudentInfo studentInfo;

        @Override
        public StudentInfo map(String s) throws Exception {

            studentInfo = new StudentInfo();

            String[] split = s.split(";");
            studentInfo.setName(split[0]);
            studentInfo.setMajor(split[1]);

            String[] courses = split[2].split(",");

            for(String str:courses)
                studentInfo.getCourses().add(str);

            return studentInfo;
        }
    }


    public static class ComputeStudiesProfile implements CoGroupFunction<Person,StudentInfo,Tuple3<String,String,String>>{

        @Override
        public void coGroup(Iterable<Person> iterable, Iterable<StudentInfo> iterable1, Collector<Tuple3<String, String, String>> collector) throws Exception {

            Person person = iterable.iterator().next();

            ArrayList<StudentInfo> infos = new ArrayList<StudentInfo>();
            for(StudentInfo info : iterable1) {
                infos.add(info);
           //     System.out.println(info.getName());
            }
            if(infos.size()>0)
                update(person, infos, collector);

            collector.collect(new Tuple3<String, String, String>(person.getName(),String.valueOf(person.getAge()),String.valueOf(person.getSex())));
        }

        public void update(Person person, Collection<StudentInfo> infos, Collector<Tuple3<String, String,String>> collector) {
            //
            person.setMajor(infos.iterator().next().getMajor());
            person.getBestCourse().add((infos.iterator().next().getCourses().get(0)));
            collector.collect(new Tuple3<String, String, String>(person.getName(),person.getMajor(),person.getBestCourse().get(0)));

        }
    }
}
