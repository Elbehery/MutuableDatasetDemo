package de.tuberlin.dima.flink.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mustafa on 18/05/15.
 */
public class StudentJobs {

	private String name;
	private List<String> jobs = new ArrayList<String>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getJobs() {
		return jobs;
	}

	public void setJobs(List<String> jobs) {
		this.jobs = jobs;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(name).append(";");

		int counter = 0;
		for(String str : jobs) {
			buffer.append(str);
			if(counter < jobs.size()-1) {
				buffer.append(";");
				counter++;
			}
		}
		return buffer.toString();
	}
}
