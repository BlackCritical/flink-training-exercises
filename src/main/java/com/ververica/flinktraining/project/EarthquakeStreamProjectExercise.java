package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.magnitude.MagnitudeHistogram;
import com.ververica.flinktraining.project.magnitude.MagnitudeNotNullFilter;
import com.ververica.flinktraining.project.model.EarthquakeCollection;
import com.ververica.flinktraining.project.model.Feature;
import com.ververica.flinktraining.project.model.Geometry;
import com.ververica.flinktraining.project.model.Location;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.nio.charset.StandardCharsets;
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
public class EarthquakeStreamProjectExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToBigEarthquakeData);
		final String inputCSV = params.get("inputCSV", ExerciseBase.pathToLocations);


		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 150; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		EarthquakeCollection earthquake = readEarthquakeFromJSON(input);
		List<Location> locationsCollection = TransformEarthquakeJSON.readLocationsFromCSV(inputCSV);

		// start the data generator
//		DataStream<Feature> earthquakes = env.addSource(new EarthquakeSource(input, maxEventDelay, servingSpeedFactor));
		DataStream<Feature> earthquakes = env.fromCollection(earthquake.features);
//		DataStreamSource<Location> locations = env.fromCollection(locationsCollection);

		SingleOutputStreamOperator<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> hist = earthquakes
			.filter(new MagnitudeNotNullFilter())
			.flatMap(new MagnitudeHistogram());

//		hist
//			.reduceGroup(new OtherEarthquakeBatchProjectExercise.GroupCountHistogram())
//			.sortPartition(value -> value.f0.f0, Order.ASCENDING)
//			.writeAsFormattedText("./output/magnitude.csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%d;%d;%d;%d;", value.f0.f0, value.f0.f1, value.f1, value.f2));
//
//		hist
//			.flatMap(new MagnitudeTypeMap())
//			.reduceGroup(new GroupCountMagnitudeType())
//			.sortPartition(value -> value.f0.f0, Order.ASCENDING)
//			.writeAsFormattedText("./output/magnitudeType.csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%d;%d;%s;%d;", value.f0.f0, value.f0.f1, value.f1, value.f2));


		DataStream<Tuple2<Feature, Integer>> filteredRides = earthquakes
				.filter(new LocationFilter())
				.flatMap(new CountAssigner())
				.keyBy(1)
				.sum(1);

		// print the filtered stream
		printOrTest(filteredRides);

		// run the cleansing pipeline
		env.execute("Earthquake Streaming");
	}

	public static EarthquakeCollection readEarthquakeFromJSON(String path) throws IOException {
		BufferedReader reader;
		InputStream gzipStream;

		gzipStream = new GZIPInputStream(new FileInputStream(path));
		reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));

		return GSON.fromJson(reader, EarthquakeCollection.class);
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

	private static class AlertLevelAndCountry extends RichFlatMapFunction<Tuple3<Double, Double, String>, Tuple2<String, String>> {

		private ListState<Location> locations;

		@Override
		public void open(Configuration config) throws Exception {
			locations = getRuntimeContext().getListState(new ListStateDescriptor<>("locations", Location.class));
		}

		@Override
		public void flatMap(Tuple3<Double, Double, String> value, Collector<Tuple2<String, String>> collector) throws Exception {
			String country = getCountry(value.f0, value.f1);
			collector.collect(new Tuple2<>(country, value.f2));
		}

		private String getCountry(double f0, double f1) throws Exception {
			double minDistance = Double.MAX_VALUE;
			String minName = "";
			for (Location location : locations.get()) {
				double distance = euklidDistance(f0, f1, location.latitude, location.longitude);
				if (distance < minDistance) {
					minDistance = distance;
					minName = location.name;
				}
			}
			return minName;
		}

		private static double euklidDistance(double p1x, double p1y, double p2x, double p2y) {
			return Math.sqrt(Math.pow(p1x - p2x, 2) + Math.pow(p1y - p2y, 2));
		}
	}
}
