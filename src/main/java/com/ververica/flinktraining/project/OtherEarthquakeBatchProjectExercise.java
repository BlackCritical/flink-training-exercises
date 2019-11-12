package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.model.EarthquakeCollection;
import com.ververica.flinktraining.project.model.Feature;
import com.ververica.flinktraining.project.model.Geometry;
import com.ververica.flinktraining.project.model.Location;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * The "Ride Cleansing" exercise from the Flink training
 * (http://training.ververica.com).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 * <p>
 * Parameters:
 * -input path-to-input-file
 */
public class OtherEarthquakeBatchProjectExercise extends ExerciseBase {

    public static final int UNDEFINED_MAGNITUDE = -9999;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", pathToALLEarthquakeData);
        final String inputCSV = params.get("inputCSV", pathToLocations);

        // set up batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        EarthquakeCollection earthquakeCollection = TransformEarthquakeJSON.readEarthquakeFromJSON(input);
        System.out.println(earthquakeCollection.features.size());

        DataSet<Feature> earthquakes = env.fromCollection(earthquakeCollection.features);

//        GroupReduceOperator<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple2<Tuple2<Integer, Integer>, Integer>> hist = earthquakes
//                .flatMap(new MagnitudeHistogram())
//                .reduceGroup(new GroupCountHistogram());

        DataSink<String> sigAndCoordinates = earthquakes
                .flatMap(new SIGAndCoordinates())
                .flatMap(new SIGAndCountry())
                .groupBy(0)
                .max(1)
                .writeAsFormattedText("./output/max-csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%d;", value.f0, value.f1))
                .name("SIGAndCoordinates");

        DataSink<String> sigAndCoordinatesSum = earthquakes
                .flatMap(new SIGAndCoordinates())
                .flatMap(new SIGAndCountry())
                .groupBy(0)
                .sum(1)
                .writeAsFormattedText("./output/sum-csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%d;", value.f0, value.f1))
                .name("SIGAndCoordinatesSum");


//        sigAndCoordinates.print();
        System.out.println("NetRuntime: " + env.execute().getNetRuntime());
    }

    private static class MagnitudeHistogram implements FlatMapFunction<Feature, Tuple2<Tuple2<Integer, Integer>, Integer>> {

        @Override
        public void flatMap(Feature value, Collector<Tuple2<Tuple2<Integer, Integer>, Integer>> out) throws Exception {
            if (value != null && value.properties != null && value.properties.mag != null) {
                double mag = value.properties.mag;
                for (int i = -1; i < 10; i++) {
                    if (i <= mag && mag < i + 1) {
                        out.collect(new Tuple2<>(new Tuple2<>(i, i + 1), 1));
                        if (i > 7) {
                            System.out.println("Extreme Case:");
                            System.out.println(value);
                        }
                        return;
                    }
                }
            } else {
                System.out.println(value);
            }
            out.collect(new Tuple2<>(new Tuple2<>(UNDEFINED_MAGNITUDE, 0), 1));
        }
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

    private static class GroupCountHistogram implements GroupReduceFunction<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple2<Tuple2<Integer, Integer>, Integer>> {
        @Override
        public void reduce(Iterable<Tuple2<Tuple2<Integer, Integer>, Integer>> values, Collector<Tuple2<Tuple2<Integer, Integer>, Integer>> out) throws Exception {
            HashMap<Integer, Integer> histValues = new HashMap<>();
            histValues.put(UNDEFINED_MAGNITUDE, 0);
            for (int i = -1; i < 10; i++) {
                histValues.put(i, 0);
            }
            values.iterator().forEachRemaining(value -> {
                int currentVal = histValues.get(value.f0.f0);
                histValues.put(value.f0.f0, currentVal + 1);
            });
            histValues.forEach((key, value) -> out.collect(new Tuple2<>(new Tuple2<>(key, key + 1), value)));
        }
    }

    private static class SIGAndCoordinates implements FlatMapFunction<Feature, Tuple3<Double, Double, Long>> {

        // out -> (latitude, longitude, alertLevel)
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
}
