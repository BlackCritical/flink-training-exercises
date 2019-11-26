package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.magnitude.GroupCountMagnitudeType;
import com.ververica.flinktraining.project.magnitude.MagnitudeHistogram;
import com.ververica.flinktraining.project.magnitude.MagnitudeNotNullFilter;
import com.ververica.flinktraining.project.magnitude.MagnitudeTypeMap;
import com.ververica.flinktraining.project.model.EarthquakeCollection;
import com.ververica.flinktraining.project.model.Feature;
import com.ververica.flinktraining.project.model.Location;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Parameters:
 * -input path-to-input-file
 */
public class OtherEarthquakeBatchProjectExercise extends ExerciseBase {

    public static final String UNDEFINED = "UNDEFINED";
    public static final int UNDEFINED_MAGNITUDE = -9999;
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

        DataSet<Feature> earthquakes = env.fromCollection(earthquakeCollection.features);

        FlatMapOperator<Feature, Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> hist = earthquakes
                .filter(new MagnitudeNotNullFilter())
                .flatMap(new MagnitudeHistogram());

        hist
                .reduceGroup(new GroupCountHistogram())
                .sortPartition(value -> value.f0.f0, Order.ASCENDING)
                .writeAsFormattedText("./output/batch/magnitude-review-status.csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%d;%d;%d;%d;", value.f0.f0, value.f0.f1, value.f1, value.f2));

        hist
                .flatMap(new MagnitudeTypeMap())
                .reduceGroup(new GroupCountMagnitudeType())
                .sortPartition(value -> value.f0.f0, Order.ASCENDING)
                .writeAsFormattedText("./output/batch/magnitudeType.csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%d;%d;%s;%d;", value.f0.f0, value.f0.f1, value.f1, value.f2));


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

        System.out.println("NetRuntime: " + env.execute().getNetRuntime() + "ms");
    }

    private static class GroupCountHistogram implements GroupReduceFunction<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>, Tuple3<Tuple2<Integer, Integer>, Integer, Integer>> {

        /**
         * @param values ->[(min_Magnitude, max_Magnitude), Magnitude Count(always 1), Magnitude Type, Reviewed Status Count (1 if reviewed else 0)] as Input
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

    private static class SIGAndCoordinates implements FlatMapFunction<Feature, Tuple3<Double, Double, Long>> {

        // out -> (latitude, longitude, SIG)
        @Override
        public void flatMap(Feature feature, Collector<Tuple3<Double, Double, Long>> collector) {
            List<Double> coordinates = feature.geometry.coordinates;
            if (!coordinates.isEmpty()) {
                collector.collect(new Tuple3<>(coordinates.get(1), coordinates.get(0), feature.properties.sig));
            }
        }
    }

    private static class SIGAndCountry implements FlatMapFunction<Tuple3<Double, Double, Long>, Tuple2<String, Long>> {

        private List<Location> locations = TransformEarthquakeJSON.readLocationsFromCSV(pathToLocations);

        private SIGAndCountry() throws IOException {
        }

        @Override
        public void flatMap(Tuple3<Double, Double, Long> value, Collector<Tuple2<String, Long>> collector) throws Exception {
            String country = getCountry(value.f0, value.f1);
            collector.collect(new Tuple2<>(country, value.f2));
        }

        private String getCountry(double f0, double f1) throws Exception {
            double minDistance = Double.MAX_VALUE;
            String minName = "";
            for (Location location : locations) {
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

    private static class MapEventsToLocationCoordinates implements FlatMapFunction<Feature, Tuple4<Double, Double, Long, Long>> {

        /**
         * @param feature       -> Earthquake Feature
         * @param collector     -> [latitude, longitude, SIG, tsunami(true/false)]
         */
        @Override
        public void flatMap(Feature feature, Collector<Tuple4<Double, Double, Long, Long>> collector) {
            List<Double> coordinates = feature.geometry.coordinates;
            if (!coordinates.isEmpty()) {
                collector.collect(new Tuple4<>(coordinates.get(1), coordinates.get(0), feature.properties.sig, feature.properties.tsunami));
            }
        }
    }

    private static class MapEventsToLocation implements FlatMapFunction<Tuple4<Double, Double, Long, Long>, Tuple3<String, Long, Long>> {

        private List<Location> locations = TransformEarthquakeJSON.readLocationsFromCSV(pathToLocations);

        private MapEventsToLocation() throws IOException {
        }

        /**
         * @param value     -> [latitude, longitude, SIG, tsunami(true/false)]
         * @param collector     -> [countryName, SIG, tsunami(true/false)]
         */
        @Override
        public void flatMap(Tuple4<Double, Double, Long, Long> value, Collector<Tuple3<String, Long, Long>> collector) {
            String country = getCountry(value.f0, value.f1);
            collector.collect(new Tuple3<>(country, value.f2, value.f3));
        }

        private String getCountry(double latitude, double longitude) {
            double minDistance = Double.MAX_VALUE;
            String minName = "";
            for (Location location : locations) {
                double distance = euklidDistance(latitude, longitude, location.latitude, location.longitude);
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
