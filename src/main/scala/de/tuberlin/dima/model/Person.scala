package de.tuberlin.dima.model

import scala.collection.mutable.ArrayBuffer

class Person{

	private var _name: String = _
	private var _school: String = _
	private var _sex: Char = _
	private var _age: Int = _
	private var _major: String = _

	// ArrayBuffer is Mutable by itself
	private val _courses: ArrayBuffer[String] = new ArrayBuffer[String]
	private val _jobs: ArrayBuffer[String] = new ArrayBuffer[String]

	def name = _name
	def name(s:String):Unit = {_name = s}

	def school = _school
	def school(s:String):Unit = {_school = s}

	def sex = _sex
	def sex(c:Char):Unit = {_sex = c}

	def age = _age
	def age(value:Int):Unit = {_age = value}

	def major = _major
	def major(s:String):Unit = {_major = s}

	def courses = _courses
	def courses(arrayBuffer: ArrayBuffer[String]): Unit = {
		_courses ++=(arrayBuffer)
	}

	def jobs = _jobs
	def jobs(arrayBuffer: ArrayBuffer[String]): Unit = {
		_jobs ++=(arrayBuffer)
	}

	override def toString: String = {
		_name + " " + _school + " " + _sex + " " + _age + " " + _major + "[" + _courses.mkString(";") + "]" + "	" + "[" + _jobs.mkString(";") + "]"
	}

}