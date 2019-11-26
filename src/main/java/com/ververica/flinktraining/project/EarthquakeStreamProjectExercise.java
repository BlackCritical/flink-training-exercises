package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.location.MapEventsToLocation;
import com.ververica.flinktraining.project.location.MapEventsToLocationCoordinates;
import com.ververica.flinktraining.project.magnitude.MagnitudeHistogram;
import com.ververica.flinktraining.project.magnitude.MagnitudeNotNullFilter;
import com.ververica.flinktraining.project.magnitude.MagnitudeTypeMap;
import com.ververica.flinktraining.project.magnitude.stream.WindowCountHistogram;
import com.ververica.flinktraining.project.magnitude.stream.WindowCountMagnitudeType;
import com.ververica.flinktraining.project.model.EarthquakeCollection;
import com.ververica.flinktraining.project.model.Feature;
import com.ververica.flinktraining.project.model.Location;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import static com.ververica.flinktraining.project.TransformEarthquakeJSON.readEarthquakeFromJSON;

/**
 * Parameters:
 * -input path-to-input-file
 */
public class EarthquakeStreamProjectExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToALLEarthquakeData);

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        EarthquakeCollection earthquake = readEarthquakeFromJSON(input);

        // start the data generator
//		DataStream<Feature> earthquakes = env.addSource(new EarthquakeSource(input, maxEventDelay, servingSpeedFactor));
        DataStream<Feature> earthquakes = env.fromCollection(earthquake.features);

        SingleOutputStreamOperator<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> hist = earthquakes
                .filter(new MagnitudeNotNullFilter())
                .flatMap(new MagnitudeHistogram());

        KeyedStream<Tuple3<Tuple2<Integer, Integer>, Integer, Integer>, Tuple> processedHist = hist
                .keyBy("f0.f0", "f0.f1")
                .countWindow(1000)
                .process(new WindowCountHistogram())
                .keyBy("f0.f0", "f0.f1");

        DataStreamSink<Tuple3<Tuple2<Integer, Integer>, Integer, Integer>> mag = processedHist
                .sum(1)
                .writeAsCsv("./output/stream/magnitude_mag", FileSystem.WriteMode.OVERWRITE, "\n", ";");

        DataStreamSink<Tuple3<Tuple2<Integer, Integer>, Integer, Integer>> reviewed = processedHist
                .sum(2)
                .writeAsCsv("./output/stream/magnitude_rev", FileSystem.WriteMode.OVERWRITE, "\n", ";");

        hist
                .flatMap(new MagnitudeTypeMap())
                .keyBy("f0.f0", "f0.f1")
                .countWindow(1000)
                .process(new WindowCountMagnitudeType())
                .writeAsCsv("./output/stream/magnitude_type", FileSystem.WriteMode.OVERWRITE, "\n", ";");


		FlatMapOperator<Tuple4<Double, Double, Long, Long>, Tuple3<String, Long, Long>> events = earthquakes
				.flatMap(new MapEventsToLocationCoordinates())
				.flatMap(new MapEventsToLocation())
				.name("Country Name, SIG, Tsunami");

		events
				.groupBy(0)
				.max(1)
				.writeAsFormattedText("./output/batch/max-sig-location-csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%d", value.f0, value.f1));

		events
				.groupBy(0)
				.sum(2)
				.writeAsFormattedText("./output/batch/max-Tsunami-location-csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%s;", value.f0, value.f2));


		// print the filtered stream
//		printOrTest(mag);

        // run the pipeline
        System.out.println("NetRuntime: " + env.execute("Earthquake Streaming").getNetRuntime() + "ms");
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
