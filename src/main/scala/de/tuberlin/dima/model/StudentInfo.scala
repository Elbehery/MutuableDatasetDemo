package de.tuberlin.dima.model

import scala.collection.mutable.ArrayBuffer

class StudentInfo {

	private var _name: String = _
	private var _major: String = _
	private val _courses: ArrayBuffer[String] = new ArrayBuffer[String]

	def name = _name
	def name(s:String):Unit = {_name = s}

	def major = _major
	def major(s:String):Unit = {_major = s}

	def courses = _courses
	def courses(arrayBuffer: ArrayBuffer[String]): Unit = {
		_courses ++=(arrayBuffer)
	}

	override def toString: String = {
		_name + " " + _major + " [" + _courses.foreach(print(_)+";") + "]"
	}

}