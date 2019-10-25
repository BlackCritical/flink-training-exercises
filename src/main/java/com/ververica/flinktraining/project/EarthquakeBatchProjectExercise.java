package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.model.Earthquake;
import com.ververica.flinktraining.project.model.Feature;
import com.ververica.flinktraining.project.model.Geometry;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

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

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 150; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		Earthquake earthquake = readEarthquakeFromJSON
	}

	private static class LocationFilter implements FilterFunction<Feature> {
		@Override
		public boolean filter(Feature value) throws Exception {
			Geometry geometry = value.geometry;
			if (geometry.type.equalsIgnoreCase("Point") && !geometry.coordinates.isEmpty()) {
				double longitude = geometry.coordinates.get(0);
				double latitude = geometry.coordinates.get(1);
//				double depth = geometry.coordinates.get(2);

				return longitude > 0 && latitude > 0;
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
}
