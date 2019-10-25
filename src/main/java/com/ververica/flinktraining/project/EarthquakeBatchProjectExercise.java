package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.model.Earthquake;
import com.ververica.flinktraining.project.model.Feature;
import com.ververica.flinktraining.project.model.Geometry;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.ververica.flinktraining.exercises.datastream_java.sources.EarthquakeSource.GSON;

/**
 * The "Ride Cleansing" exercise from the Flink training
 * (http://training.ververica.com).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 *
 * Parameters:
 *   -input path-to-input-file
 *
 */
public class EarthquakeBatchProjectExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToEarthquakeData);

		// set up batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		Earthquake earthquake = readEarthquakeFromJSON(input);

		DataSet<Feature> earthquakes = env.fromCollection(earthquake.features);
        System.out.println(earthquakes.count());

        DataSet<Tuple2<Feature, Integer>> filteredRides = earthquakes
			// filter out earthquakes that do not start or stop in NYC
			.filter(new LocationFilter())
			.flatMap(new CountAssigner())
			.groupBy(1)
			.reduce(new CountReducer());

		filteredRides.print();
	}

	public static Earthquake readEarthquakeFromJSON(String path) throws IOException {
		BufferedReader reader;
		InputStream gzipStream;

		gzipStream = new GZIPInputStream(new FileInputStream(path));
		reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));

		return GSON.fromJson(reader, Earthquake.class);
	}

	private static class LocationFilter implements FilterFunction<Feature> {
		@Override
		public boolean filter(Feature value) throws Exception {
			Geometry geometry = value.geometry;
			if (geometry.type.equalsIgnoreCase("Point") && !geometry.coordinates.isEmpty()) {
				double longitude = geometry.coordinates.get(0);
				double latitude = geometry.coordinates.get(1);
//				double depth = geometry.coordinates.get(2);

//				return longitude > 10 && latitude > 10;
				return 47.40723 < latitude && latitude < 54.908 && 5.98814 < longitude && longitude < 14.98854;  // GERMANY
			}
			return false;
		}
	}

	private static class CountAssigner implements FlatMapFunction<Feature, Tuple2<Feature, Integer>> {
		@Override
		public void flatMap(Feature value, Collector<Tuple2<Feature, Integer>> out) throws Exception {
			out.collect(new Tuple2<>(value, 1));
		}
	}

	public static class CountReducer implements ReduceFunction<Tuple2<Feature, Integer>> {
		@Override
		public Tuple2<Feature, Integer> reduce(Tuple2<Feature, Integer> firstTuple, Tuple2<Feature, Integer> secondTuple) throws Exception {
			return new Tuple2<>(firstTuple.f0, firstTuple.f1 + secondTuple.f1);
		}
	}

	private static class ReduceGroup implements GroupReduceFunction<Tuple2<Feature, Integer>, Tuple2<Feature, Integer>> {
		@Override
		public void reduce(Iterable<Tuple2<Feature, Integer>> values, Collector<Tuple2<Feature, Integer>> out) throws Exception {
			List<Tuple2<Feature, Integer>> list = new ArrayList<>();
			values.iterator().forEachRemaining(list::add);
			out.collect(new Tuple2<>(list.get(0).f0, list.size()));
		}
	}
}
