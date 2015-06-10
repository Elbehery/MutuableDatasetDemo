package eu.stratosphere.emma.stateful.baseline;

import de.tuberlin.dima.flink.model.Person;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import java.lang.reflect.Field;
import java.util.*;

public class Stateful {

	private static String STATEFUL_BASE_PATH = String.format("%s/emma/stateful", System.getProperty("java.io.tmpdir"));

	private static final String TASK_INFO_ACC_PREFIX = "stateful.map";

	/**
	 * An abstraction for a stateful dataset which allows restricted point-wise updates.
	 *
	 * @param <A> Element type for the dataset.
	 * @param <K> Key type dataset elements.
	 */
	public static class Set<A, K extends Comparable<K>> {

		private final UUID uuid = UUID.randomUUID();
		private final String path = String.format("%s/%s", STATEFUL_BASE_PATH, uuid);
		private final ExecutionEnvironment env;
		private final KeySelector<A, K> statefulKey;
		private final ArrayList<Tuple2<Integer, String>> taskAssignmentMap;
		private final TypeInformation<A> typeInformation;

		/**
		 * Convert the stateless dataset into a stateful one.
		 *
		 * @param stateless The stateless dataset to be converted.
		 * @param key       The key selector to be used for hashing.
		 * @throws Exception
		 */
		public Set(ExecutionEnvironment env, DataSet<A> stateless, KeySelector<A, K> key) throws Exception {
			// create dataflow
			String taskAssignmentAccumulatorName = String.format("%s.%s", TASK_INFO_ACC_PREFIX, uuid);
			stateless
					.partitionByHash(key)
					.map(new HostTrackingMapper<A>(taskAssignmentAccumulatorName))
					.write(new TypeSerializerOutputFormat<A>(), path, FileSystem.WriteMode.NO_OVERWRITE);

			// execute dataflow
			JobExecutionResult result = env.execute("Stateful[Create]");

			// fetch
			this.env = env;
			this.statefulKey = key;
			this.taskAssignmentMap = result.getAccumulatorResult(taskAssignmentAccumulatorName);
			this.typeInformation = stateless.getType();
		}

		public <B, C> DataSet<Person> updateWith(FlatMapFunction<Tuple2<A, B>, C> udf, DataSet<B> updates, KeySelector<B, K> updateKey) {

			DataSet<Person> res = env.readFile(new TypeSerializerInputFormat<A>(typeInformation), path)
					.coGroup(updates)
					.where(statefulKey).equalTo(updateKey).with(new StatefulUpdater<A, B, Person>());


			return res;
		}
	}


	// private static class StatefulUpdater<A, B, C> implements CoGroupFunction<A, B, C> {
	private static class StatefulUpdater<A, B, C> implements CoGroupFunction<A, B, Person> {

		private Map<Integer, Class<?>> fieldsMap;

		@Override
		public void coGroup(Iterable<A> first, Iterable<B> second, Collector<Person> out) throws Exception {


			A a = null;
			Iterator<A> aIterator = first.iterator();
			if (aIterator.hasNext()) {
				a = aIterator.next();
			}

			if (a != null) {
				ArrayList<B> bList = new ArrayList<B>();
				Iterator<B> bIterator = second.iterator();

				while (bIterator.hasNext())
					bList.add(bIterator.next());

				if (bList.size() > 0) {
					this.fieldsMap = getAllFields(bList.get(0));
					update(a, bList, out, this.fieldsMap);
				}
			}
		}

		public void update(A a, Collection<B> bCollection, Collector<Person> collector, Map<Integer, Class<?>> fieldsMap) {

			try {
				// retrieve all the fields of A to check the corresponding fields in B
				Field[] fields = a.getClass().getDeclaredFields();

				for (Field field : fields) {

					int fieldHashCode = field.getName().hashCode();

					//check that the hashCode of the field exist in the map, If TRUE, compare the TYPE of each of them
					if (fieldsMap.containsKey(fieldHashCode)) {
						if (field.getType().equals(fieldsMap.get(fieldHashCode))) {

							// allowing access to private fields
							field.setAccessible(true);

							//check the type of the field .. If Collection, add all object's values to it. Else, fill it only once
							if (List.class.isAssignableFrom(field.getType())) {

								// Update the field of A with the value in B ..
								for (B b : bCollection) {
									Field bField = b.getClass().getDeclaredField(field.getName());
									bField.setAccessible(true);

									// FIXME: How to edit a List with Reflection ??
									List<String> aa = (List<String>)field.get(a);
									aa.add(((List<String>)bField.get(b)).get(0));

									field.set(a, aa);
								}

							}
							else {

								B b = bCollection.iterator().next();
								Field bField = b.getClass().getDeclaredField(field.getName());
								bField.setAccessible(true);
								field.set(a, bField.get(b));
							}

						}
					}
				}
			} catch (NoSuchFieldException | IllegalAccessException ex) {
				System.out.println(ex.getMessage());
			}
		}


		// Creating a HashMap holds all the fields of the given object, to check which fields to updates in the original object
		public <T> Map<Integer, Class<?>> getAllFields(T t) {

			Map<Integer, Class<?>> fieldsMap = new HashMap<Integer, Class<?>>();
			Field[] fields = t.getClass().getDeclaredFields();
			for (Field field : fields) {
				fieldsMap.put(field.getName().hashCode(), field.getType());
			}

			return fieldsMap;
		}


	}

	/**
	 * A special mapper that tracks the placement of an execution vertex. Used just before the write task in order
	 * to obtain a "taskID -> hostName" map for a stateful dataset.
	 *
	 * @param <A> Element type for the dataset.
	 */
	private static class HostTrackingMapper<A> extends RichMapFunction<A, A> {

		private String taskAssignmentAccumulatorName;

		public HostTrackingMapper(String taskAssignmentAccumulatorName) {
			this.taskAssignmentAccumulatorName = taskAssignmentAccumulatorName;
		}

		private TaskAssignmentAccumulator taskAssignmentAccumulator = new TaskAssignmentAccumulator();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			RuntimeContext ctx = getRuntimeContext();

			// adding the host info to the local accumulator
			// FIXME: we need to get the proper hostname via the RuntimeContext. If this is not possible at the moment, open a PR. This is a blocker and needs to be resolved ASAP.
			Tuple2<Integer, String> hostInfo = new Tuple2<Integer, String>(getRuntimeContext().getIndexOfThisSubtask(), "localhost");
			this.taskAssignmentAccumulator.add(hostInfo);

			// register the accumulator instance
			getRuntimeContext().addAccumulator(taskAssignmentAccumulatorName, this.taskAssignmentAccumulator);

		}

		@Override
		public A map(A value) throws Exception {
			return value;
		}
	}


	// Accumulator to gather information about the task-manager, in order to materialize records of the same node in the same file
	public static class TaskAssignmentAccumulator implements Accumulator<Tuple2<Integer, String>, ArrayList<Tuple2<Integer, String>>> {

		private ArrayList<Tuple2<Integer, String>> hostInfo;

		public TaskAssignmentAccumulator() {
			this.hostInfo = new ArrayList<Tuple2<Integer, String>>();
		}

		@Override
		public void add(Tuple2<Integer, String> value) {
			this.hostInfo.add(value);
		}

		@Override
		public ArrayList<Tuple2<Integer, String>> getLocalValue() {
			return this.hostInfo;
		}

		@Override
		public void resetLocal() {
			this.hostInfo.clear();
		}

		@Override
		public void merge(Accumulator<Tuple2<Integer, String>, ArrayList<Tuple2<Integer, String>>> other) {
			this.hostInfo.addAll(other.getLocalValue());
		}

		@Override
		public Accumulator<Tuple2<Integer, String>, ArrayList<Tuple2<Integer, String>>> clone() {

			TaskAssignmentAccumulator clonedTaskAssignmentAccumulator = new TaskAssignmentAccumulator();
			clonedTaskAssignmentAccumulator.hostInfo = this.hostInfo;
			return clonedTaskAssignmentAccumulator;
		}
	}
}