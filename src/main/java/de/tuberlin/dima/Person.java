package de.tuberlin.dima;

import java.io.Serializable;
import java.util.*;

public class Person implements Serializable{

    private String name;
    private String school;
    private char sex;
    private int age;
    private String major = null;
    private List<String> bestCourse = new ArrayList<String>();

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

    @Override
    public String toString() {
        StringBuffer person = new StringBuffer().append(name).append(";")
                .append(school).append(";").append(sex).append(";")
                .append(age).append(";").append(major).append(";");

        for(String course : bestCourse){
            person.append(course).append(";");
        }
        return person.toString();
    }
}
