package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.location.MapEventsToLocation;
import com.ververica.flinktraining.project.location.MapEventsToLocationCoordinates;
import com.ververica.flinktraining.project.magnitude.GroupCountMagnitudeType;
import com.ververica.flinktraining.project.magnitude.MagnitudeHistogram;
import com.ververica.flinktraining.project.magnitude.MagnitudeNotNullFilter;
import com.ververica.flinktraining.project.magnitude.MagnitudeTypeMap;
import com.ververica.flinktraining.project.model.EarthquakeCollection;
import com.ververica.flinktraining.project.model.Feature;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Parameters:
 * -input path-to-input-file
 */
public class EarthquakeBatchProjectExercise extends ExerciseBase {

    public static final String UNDEFINED = "UNDEFINED";
    public static final int MIN_MAGNITUDE = -10;
    public static final int MAX_MAGNITUDE = 10;

    public static int[] MAGNITUDES = new int[MAX_MAGNITUDE - MIN_MAGNITUDE + 1];

    static {
        int j = 0;
        for (int i = MIN_MAGNITUDE; i < MAX_MAGNITUDE; i++) {
            MAGNITUDES[j++] = i;
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", pathToALLEarthquakeData);

        EarthquakeCollection earthquakeCollection = TransformEarthquakeJSON.readEarthquakeFromJSON(input);

        // set up batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        DataSet<Feature> earthquakes = env.fromCollection(earthquakeCollection.features);  // We are only interested in the Features

        // Map to DataSet, which contains a min- and maxMagnitude per row (for example 5 and 6 to represent a magnitude between 5 and 6)
        // Those min Max values are stored inside the Tuple2
        // Additionally data:
        // f1 -> the frequency of magnitudes for the range given inside the Tuple2 (at this point still always 1)
        // f2 -> the magnitudeType
        // f3 -> the frequency of reviewed earthquakes for the range given inside the Tuple2 (at this point still always 1 or 0)
        FlatMapOperator<Feature, Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> hist = earthquakes
                .filter(new MagnitudeNotNullFilter())
                .flatMap(new MagnitudeHistogram());

        // discard f2 -> the magnitudeType from the input
        // Add up the magnitude and reviewed frequencies
        // We will end up with an CSV, which contains every MagnitudeRange exactly once.
        // For every Range: the frequency of earthquakes within this magnitude range AND how many of them got reviewed by a human, will be computed
        hist
                .reduceGroup(new GroupCountHistogram())
                .sortPartition(value -> value.f0.f0, Order.ASCENDING)
                .writeAsFormattedText("./output/batch/magnitude-review-status.csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%d;%d;%d;%d", value.f0.f0, value.f0.f1, value.f1, value.f2));

        // discard f3 -> the reviewed frequency from the input
        // Add up the magnitude frequencies per combination of range and type
        // We will end up with an CSV, which contains every MagnitudeRange 11 times once per Magnitude Type (you can find all at com.ververica.flinktraining.project.util.MagnitudeType).
        // For every Range and Type combination: the frequency of earthquakes within this magnitude range and for this specific Magnitude Type, will be computed
        hist
                .flatMap(new MagnitudeTypeMap())
                .reduceGroup(new GroupCountMagnitudeType())
                .sortPartition(value -> value.f0.f0, Order.ASCENDING)
                .writeAsFormattedText("./output/batch/magnitudeType.csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%d;%d;%s;%d", value.f0.f0, value.f0.f1, value.f1, value.f2));


        FlatMapOperator<Tuple4<Double, Double, Long, Long>, Tuple3<String, Long, Long>> events = earthquakes
                .flatMap(new MapEventsToLocationCoordinates()) // Simple mapping from Feature to -> [latitude, longitude, SIG, tsunami(1/0)]
                .flatMap(new MapEventsToLocation()) // Map the longitude and latitude to the country with the smallest euclidean distance to the given latitude and longitude
                .name("Country Name, SIG, Tsunami");

        // compute the maximum SIG-Number for every country
        // discard f2 -> the Tsunami Count
        // We will end up with an CSV for every node.
        // They can be combined into one single file with the Windows CMD command:
        // copy 1 + 2 + 3 + 4 output.csv
        events
                .groupBy(0)
                .max(1)
                .writeAsFormattedText("./output/batch/max-sig-location", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%d", value.f0, value.f1));

        // compute the frequency of Tsunami events for every country
        // discard f1 -> the SIG-Number
        // We will end up with an CSV for every node.
        // They can be combined into one single file with the Windows CMD command:
        // copy 1 + 2 + 3 + 4 output.csv
        events
                .groupBy(0)
                .sum(2)
                .writeAsFormattedText("./output/batch/max-Tsunami-location", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%s", value.f0, value.f2));

        System.out.println("NetRuntime: " + env.execute().getNetRuntime() + "ms");
    }

    private static class GroupCountHistogram implements GroupReduceFunction<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>, Tuple3<Tuple2<Integer, Integer>, Integer, Integer>> {

        /**
         * @param values -> [(min_Magnitude, max_Magnitude), Magnitude Count(always 1), Magnitude Type, Reviewed Status Count (1 if reviewed else 0)] as Input
         * @param out    -> [(min_Magnitude, max_Magnitude), Magnitude Count, Review Status]
         */
        @Override
        public void reduce(Iterable<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> values, Collector<Tuple3<Tuple2<Integer, Integer>, Integer, Integer>> out) throws Exception {
            HashMap<Integer, Tuple2<Integer, Integer>> histValues = new HashMap<>();  // Map RangeBegin -> (count of magnitudes in this Range, count of status in this Range)
            for (int minMagnitude : MAGNITUDES) {
                histValues.put(minMagnitude, new Tuple2<>(0, 0));  // init
            }
            values.iterator().forEachRemaining(value -> {
                Tuple2<Integer, Integer> currentVal = histValues.get(value.f0.f0);
                currentVal.f0 += value.f1;  // Increase Mag Count
                currentVal.f1 += value.f3;  // Increase Reviewed Count
                histValues.put(value.f0.f0, currentVal);
            });
            histValues.forEach((key, value) -> out.collect(new Tuple3<>(new Tuple2<>(key, key + 1), value.f0, value.f1)));
        }
    }
}
