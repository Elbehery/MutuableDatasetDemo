package de.tuberlin.dima.model;

import java.io.Serializable;
import java.util.*;

public class Person implements Serializable{

    private String name;
    private String school;
    private char sex;
    private int age;
    private String major = "";
    private List<String> bestCourse = new ArrayList<String>();
    private List<String> jobs = new ArrayList<String>();

    public String getMajor() {
        return major;
    }

    public void setMajor(String major) {
        this.major = major;
    }

    public List<String> getBestCourse() {
        return bestCourse;
    }

    public void setBestCourse(List<String> bestCourse) {
        this.bestCourse = bestCourse;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }

    public char getSex() {
        return sex;
    }

    public void setSex(char sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public List<String> getJobs() {
        return jobs;
    }

    public void setJobs(List<String> jobs) {
        this.jobs = jobs;
    }

    @Override
    public String toString() {
        StringBuffer person = new StringBuffer().append(name).append(";")
                .append(school).append(";").append(sex).append(";")
                .append(age).append(";").append(major).append(";");

        int counter = 0;
        for(String course : bestCourse){
            person.append(course);
            if(counter < bestCourse.size()-1) {
                person.append(";");
                counter++;
            }
        }

        counter = 0;
        for(String job : jobs){
            person.append(job);
            if(counter < jobs.size()-1) {
                person.append(";");
                counter++;
            }
        }
        return person.toString();
    }
}
