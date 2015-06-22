package de.tuberlin.dima.model

import scala.collection.mutable.ArrayBuffer

class StudentJobs {

	private var _name: String = _
	private val _jobs: ArrayBuffer[String] = new ArrayBuffer[String]

	def name = _name
	def name(s:String):Unit = {_name = s}

	def jobs = _jobs
	def jobs(arrayBuffer: ArrayBuffer[String]): Unit = {
		_jobs ++=(arrayBuffer)
	}

	override def toString: String = {
		_name + " [" + _jobs.foreach(print(_)+";") + "]"
	}

}