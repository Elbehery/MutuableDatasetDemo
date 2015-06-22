package de.tuberlin.dima.partitioning

import de.tuberlin.dima.model.{Person, StudentInfo, StudentJobs}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer


object MutableDatasetPartitioning {

	def main(args: Array[String]) {

		// set up the execution environment
		val env = ExecutionEnvironment.getExecutionEnvironment

		// read the person data
		val inPerson: DataSet[(String, Person)] = env.readTextFile("../MutuableDatasetDemo/src/main/resources/Person.csv").map(personMapper(_)).map(x => (x name, x))
		val inStudent: DataSet[(String, StudentInfo)] = env.readTextFile("../MutuableDatasetDemo/src/main/resources/StudentInfo.csv").map(studentInfoMapper(_)).map(x => (x name, x))
		val inJobs: DataSet[(String, StudentJobs)] = env.readTextFile("../MutuableDatasetDemo/src/main/resources/Jobs.csv").map(studentJobMapper(_)).map(x => (x name, x))

		val updatedPersonOne: DataSet[Person] = inPerson.coGroup(inStudent).where(0).equalTo(0) {
			(inPerson, inStudent, out: Collector[Person]) => {

				val person = inPerson map (_._2) toSet
				val students = inStudent map (_._2) toSet

				for (x <- person) {
					for (y <- students) {
						x major (y major)
						x.courses(y.courses)
					}
					out.collect(x)
				}
			}
		}

		val updatedPersonTwo: DataSet[Person] = updatedPersonOne.map(x=>(x name, x)).coGroup(inJobs).where(0).equalTo(0) {
			(inPerson, inJobs, out: Collector[Person]) => {

				val person = inPerson map (_._2) toSet
				val jobs = inJobs map (_._2) toSet

				for (x <- person) {
					for (y <- jobs) {
						x.jobs(y.jobs)
					}
					out.collect(x)
				}
			}
		}

		// emit result
		updatedPersonTwo.print()

		// execute program
		env.execute("WordCount Example")
	}

	def personMapper(txt: String): Person = {

		val splits = txt.split(";").toList
		val person = new Person
		person name(splits(0))
		person school(splits(1))
		person sex((splits(2).charAt(0)))
		person age(splits(3).toInt)

		person
	}

	def studentInfoMapper(txt: String): StudentInfo = {

		val splits = txt.split(";").toList
		val studentInfo = new StudentInfo
		studentInfo name (splits(0))
		studentInfo major (splits(1))

		// FIXME: ArrayBuffer should be Mutable .. However, I could not add elemnts to it, I had to assign a new buffer each time !!!!
		studentInfo.courses.+=((splits(2)))

		studentInfo
	}

	def studentJobMapper(txt: String): StudentJobs = {

		val splits = txt.split(";").toList
		val studentJobs = new StudentJobs
		studentJobs name(splits(0))
		studentJobs.jobs.+=(splits(1))

		studentJobs
	}

}
