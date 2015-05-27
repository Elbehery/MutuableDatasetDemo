package de.tuberlin.dima.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class StudentInfo implements Serializable {

    private String name;
    private String major;
    private List<String> courses;

    public StudentInfo(){
        courses = new ArrayList<String>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMajor() {
        return major;
    }

    public void setMajor(String major) {
        this.major = major;
    }

    public List<String> getCourses() {
        return courses;
    }

    public void setCourses(List<String> courses) {
        this.courses = courses;
    }
}
