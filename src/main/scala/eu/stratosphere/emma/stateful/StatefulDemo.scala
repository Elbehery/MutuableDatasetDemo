package eu.stratosphere.emma.stateful



import de.tuberlin.dima.model.Person
import org.apache.flink.api.scala._

object StatefulDemo {

	def main(args: Array[String]) {

		val env = ExecutionEnvironment.getExecutionEnvironment
		val inPerson: DataSet[Person] = env.readTextFile("../MutuableDatasetDemo/src/main/resources/Person.csv").map(personMapper(_))
		val set = new Set[Person,String](env,inPerson,keyExtractor(_:Person))

		env.execute("tst Stateful")
	}

	def keyExtractor(person: Person):String = person name

	def personMapper(txt: String): Person = {

		val splits = txt.split(";").toList
		val person = new Person
		person name (splits(0))
		person school (splits(1))
		person sex ((splits(2).charAt(0)))
		person age (splits(3).toInt)

		person
	}

}
