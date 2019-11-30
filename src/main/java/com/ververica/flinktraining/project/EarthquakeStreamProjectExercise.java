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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.ververica.flinktraining.project.TransformEarthquakeJSON.readEarthquakeFromJSON;

/**
 * Parameters:
 * -input path-to-input-file
 */
public class EarthquakeStreamProjectExercise extends ExerciseBase {
    
    private static final int WINDOW_SIZE = 10;
    private static final String FIRST_COORDINATE = "f0.f0";
    private static final String SECOND_COORDINATE = "f0.f1";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToALLEarthquakeData);

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        EarthquakeCollection earthquakeCollection = readEarthquakeFromJSON(input);

        DataStream<Feature> earthquakes = env.fromCollection(earthquakeCollection.features);  // We are only interested in the Features

        // Map to DataSet, which contains a min- and maxMagnitude per row (for example 5 and 6 to represent a magnitude between 5 and 6)
        // Those min Max values are stored inside the Tuple2
        // Additionally data:
        // f1 -> the frequency of magnitudes for the range given inside the Tuple2 (at this point still always 1)
        // f2 -> the magnitudeType
        // f3 -> the frequency of reviewed earthquakes for the range given inside the Tuple2 (at this point still always 1 or 0)
        SingleOutputStreamOperator<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> hist = earthquakes
                .filter(new MagnitudeNotNullFilter())
                .keyBy("properties.status")
                .flatMap(new MagnitudeHistogram());


        // key the stream by the coordinates provided in f0 inside a Tuple2
        // discard f2 -> the magnitudeType from the input
        // Add up the magnitude and reviewed frequencies per window
        KeyedStream<Tuple3<Tuple2<Integer, Integer>, Integer, Integer>, Tuple> processedHist = hist
                .keyBy(FIRST_COORDINATE, SECOND_COORDINATE)
                .countWindow(WINDOW_SIZE)
                .process(new WindowCountHistogram())
                .keyBy(FIRST_COORDINATE, SECOND_COORDINATE);


        // Add up the magnitude frequencies for every partial result (every window result)
        // We will end up with an CSV for every node used
        // For every Range: the frequency of earthquakes within this magnitude range, will be computed
        // BUT all partial results will also be contained inside the result CSV
        // To create one single CSV File with no duplicated ranges we will need to execute com.ververica.flinktraining.project.CollectStreamData
        // Afterwards we will find a output.csv inside ./output/stream/magnitude_mag
        DataStreamSink<Tuple3<Tuple2<Integer, Integer>, Integer, Integer>> mag = processedHist
                .sum(1)
                .writeAsCsv("./output/stream/magnitude_mag", FileSystem.WriteMode.OVERWRITE, "\n", ";");


        // Add up the reviewed frequencies for every partial result (every window result)
        // We will end up with an CSV for every node used
        // For every Range: the frequency of earthquakes within this magnitude range which got reviewed by a human, will be computed
        // BUT all partial results will also be contained inside the result CSV (one for every window)
        // To create one single CSV File with no duplicated ranges we will need to execute com.ververica.flinktraining.project.CollectStreamData
        // Afterwards we will find a output.csv inside ./output/stream/magnitude_rev
        DataStreamSink<Tuple3<Tuple2<Integer, Integer>, Integer, Integer>> reviewed = processedHist
                .sum(2)
                .writeAsCsv("./output/stream/magnitude_rev", FileSystem.WriteMode.OVERWRITE, "\n", ";");

        // discard f3 -> the reviewed frequency from the input
        // Add up the magnitude frequencies per combination of range and type for every window
        // We will end up with an CSV for every node,
        // which contains every MagnitudeRange 11 times once per Magnitude Type (you can find all at com.ververica.flinktraining.project.util.MagnitudeType) and once per window.
        // For every Range and Type combination: the frequency of earthquakes within this magnitude range and for this specific Magnitude Type, will be computed
        // BUT all partial results will also be contained inside the result CSV (one for every window)
        // To create one single CSV File with no duplicated ranges we will need to execute com.ververica.flinktraining.project.CollectStreamData
        // Afterwards we will find a output.csv inside ./output/stream/magnitude_type
        hist
                .flatMap(new MagnitudeTypeMap())
                .keyBy(FIRST_COORDINATE, SECOND_COORDINATE)
                .countWindow(WINDOW_SIZE)
                .process(new WindowCountMagnitudeType())
                .writeAsCsv("./output/stream/magnitude_type", FileSystem.WriteMode.OVERWRITE, "\n", ";");


        SingleOutputStreamOperator<Tuple3<String, Long, Long>> events = earthquakes
                .flatMap(new MapEventsToLocationCoordinates()) // Simple mapping from Feature to -> [latitude, longitude, SIG, tsunami(1/0)]
                .flatMap(new MapEventsToLocation()) // Map the longitude and latitude to the country with the smallest euclidean distance to the given latitude and longitude
                .name("Country Name, SIG, Tsunami");

        // key by the country
        // compute the maximum SIG-Number for every country
        // the Tsunami Count will also be contained inside this result, but will be ignored afterwards
        events
				.keyBy(0)
				.max(1)
                .writeAsCsv("./output/stream/max-sig-location", FileSystem.WriteMode.OVERWRITE, "\n", ";");

        // key by the country
        // compute the frequency of Tsunami events for every country
        // the SIG-Number will also be contained inside this result, but will be ignored afterwards
		events
				.keyBy(0)
				.sum(2)
                .writeAsCsv("./output/stream/sum-Tsunami-location", FileSystem.WriteMode.OVERWRITE, "\n", ";");


		// print the filtered stream
//		printOrTest(mag);

        // run the pipeline
        System.out.println("NetRuntime: " + env.execute("Earthquake Streaming").getNetRuntime() + "ms");
    }
}
