package eu.stratosphere.emma.stateful.baseline


import eu.stratosphere.emma.stateful.baseline.Stateful.Set
import org.apache.flink.api.java.{ExecutionEnvironment, DataSet}
import org.apache.flink.api.java.functions.KeySelector


class StatefulAdapter[A,K <: Comparable[K]](env: ExecutionEnvironment, stateless: DataSet[A], key: KeySelector[A, K]) {

	val stateful = new Set[A,K](env,stateless,key)

	def updateResultMapper[A,C](dataSet:DataSet[Either[A, C]]):DataSet[A] = ???

}
