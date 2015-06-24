package eu.stratosphere.emma.stateful


import java.util.UUID

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.reflect.ClassTag


class Set[A: TypeInformation : ClassTag, K: TypeInformation : ClassTag](executionEnvironment: ExecutionEnvironment, val dataSet: DataSet[A], keyFunA: A => K) {

	val STATEFUL_BASE_PATH: String = String.format("%s/emma/stateful", System.getProperty("java.io.tmpdir"))
	val TASK_INFO_ACC_PREFIX: String = "stateful.map"
	val uuid: UUID = UUID.randomUUID
	val path: String = String.format("%s/%s", STATEFUL_BASE_PATH, uuid)

	this.dataSet.partitionByHash(keyFunA).write(new TypeSerializerOutputFormat[A], path, FileSystem.WriteMode.OVERWRITE)


	def updateWith[B: TypeInformation : ClassTag, C: TypeInformation : ClassTag](udf: FlatMapFunction[Tuple2[A, Seq[B]], C], updates: DataSet[B], keyFunB: B => K): DataSet[Either[A, C]] = {

		val leftDataSet = executionEnvironment.readFile(new TypeSerializerInputFormat[A](dataSet.getType()), path);
	//	val res: DataSet[Either[A, C]] = leftDataSet.coGroup(updates).where(keyFunA).equalTo(keyFunB);

		/*{
			(leftDataSet, updates, out: Collector[C]) => {
				//TODO: I should here perform the UDF
				out.collect()

			};
*/
				/*val res[C] = executionEnvironment.readFile(new TypeSerializerInputFormat[A](TypeInformation[A]), path).coGroup(updates).where(funA).equalTo(funB) {
					(this.dataSet, updates,out: Collector[C]) =>{
						out.collect()
						//out.collect(l.max, r.min))
					}
				}*/
				//	updates.coGroup(dataSet).where(funB).equalTo(funA).withForwardedFields()

				return null
//		}

	}
}
