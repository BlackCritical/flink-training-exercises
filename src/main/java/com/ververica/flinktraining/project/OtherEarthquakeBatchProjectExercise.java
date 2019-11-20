package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.model.EarthquakeCollection;
import com.ververica.flinktraining.project.model.Feature;
import com.ververica.flinktraining.project.model.Geometry;
import com.ververica.flinktraining.project.model.Location;
import com.ververica.flinktraining.project.util.MagnitudeType;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
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

    private static final String UNDEFINED = "UNDEFINED";
    public static final int UNDEFINED_MAGNITUDE = -9999;
    public static final int MIN_MAGNITUDE = -10;
    public static final int MAX_MAGNITUDE = 10;
    public static final double NULL_VALUE = -99999;

    private static int[] MAGNITUDES = new int[MAX_MAGNITUDE - MIN_MAGNITUDE + 1];

    static {
        int j = 0;
        for (int i = MIN_MAGNITUDE; i < MAX_MAGNITUDE; i++) {
            MAGNITUDES[j++] = i;
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", pathToBigEarthquakeData);
//        final String inputCSV = params.get("inputCSV", pathToLocations);

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
                .writeAsFormattedText("./output/magnitude.csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%d;%d;%d;%d;", value.f0.f0, value.f0.f1, value.f1, value.f2));

        hist
                .flatMap(new MagnitudeTypeMap())
                .reduceGroup(new GroupCountMagnitudeType())
                .sortPartition(value -> value.f0.f0, Order.ASCENDING)
                .writeAsFormattedText("./output/magnitudeType.csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%d;%d;%s;%d;", value.f0.f0, value.f0.f1, value.f1, value.f2));

//        DataSink<String> sigAndCoordinates = earthquakes
//                .flatMap(new SIGAndCoordinates())
//                .flatMap(new SIGAndCountry())
//                .groupBy(0)
//                .max(1)
//                .writeAsFormattedText("./output/max-csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%d;", value.f0, value.f1))
//                .name("SIGAndCoordinates");

//        DataSink<String> sig = earthquakes
//                .flatMap(new PlaceSIGAndCoordinates())
//                .flatMap(new PlaceSIGAndCountry())
//                .groupBy(0)
//                .max(1)
//                .writeAsFormattedText("./output/max-location-csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%s;%d;", value.f1, value.f0, value.f2))
//                .name("Location, SIG And Coordinates");

//        FlatMapOperator<Tuple5<Double, Double, Long, Double, Long>, Tuple4<String, Long, Double, Long>> events = earthquakes
//                .flatMap(new MapEventsToLocationCoordinates())
//                .flatMap(new MapEventsToLocation())
//                .name("Latitude, longitude, SIG, magType, tsunami");
//
//        events
//                .groupBy(0)
//                .max(1)
//                .writeAsFormattedText("./output/max-sig-location-csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%d", value.f0, value.f1));

//        events
//                .groupBy(0)
//                .sum(3)
//                .writeAsFormattedText("./output/max-Tsunami-location-csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%s;", value.f0, value.f3));
//        events.writeAsFormattedText("./output/max-MAG-Type-location-csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%s;", value.f0, value.f2));

//        DataSink<String> sigAndCoordinatesSum = earthquakes
//                .flatMap(new SIGAndCoordinates())
//                .flatMap(new SIGAndCountry())
//                .groupBy(0)
//                .sum(1)
//                .writeAsFormattedText("./output/sum-csv", FileSystem.WriteMode.OVERWRITE, value -> String.format("%s;%d;", value.f0, value.f1))
//                .name("SIGAndCoordinatesSum");


//        sigAndCoordinates.print();
        System.out.println("NetRuntime: " + env.execute().getNetRuntime() + "ms");
    }

    private static class MagnitudeHistogram implements FlatMapFunction<Feature, Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> {

        /**
         * @param value Feature as Input
         * @param out   -> [(min_Magnitude, max_Magnitude), Magnitude Count(always 1), Magnitude Type, Reviewed Status Count (1 if reviewed else 0)]
         */
        @Override
        public void flatMap(Feature value, Collector<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> out) {
            String magType = value.properties.magType;
            String reviewStatus = value.properties.status;
            double mag = value.properties.mag;

            for (int minMagnitude : MAGNITUDES) {
                if (minMagnitude <= mag && mag < minMagnitude + 1) {  // find correct range
                    magType = magType != null ? magType : UNDEFINED;
                    out.collect(new Tuple4<>(new Tuple2<>(minMagnitude, minMagnitude + 1), 1, magType, isReviewed(reviewStatus)));

                    if (minMagnitude >= 8) {
                        System.out.println("Extreme Case:");
                        System.out.println(value);
                    }
                    return;
                }
            }
        }

        /**
         * @return 1 if review else 0
         */
        private int isReviewed(String reviewStatus) {
            return reviewStatus != null ? reviewStatus.equalsIgnoreCase("reviewed") ? 1 : 0 : 0;
        }
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

    private static class MagnitudeNotNullFilter implements FilterFunction<Feature> {
        @Override
        public boolean filter(Feature value) throws Exception {
            return value.properties.mag != null;
        }
    }

    private static class MagnitudeTypeMap implements FlatMapFunction<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>, Tuple3<Tuple2<Integer, Integer>, String, Integer>> {

        /**
         * @param value -> [(min_Magnitude, max_Magnitude), Magnitude Count(always 1), Magnitude Type, Reviewed Status Count (1 if reviewed else 0)] as Input
         * @param out   -> [(min_Magnitude, max_Magnitude), Magnitude Type, Count(always 1)] as Output
         */
        @Override
        public void flatMap(Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer> value, Collector<Tuple3<Tuple2<Integer, Integer>, String, Integer>> out) throws Exception {
            out.collect(new Tuple3<>(value.f0, value.f2, 1));
        }
    }

    private static class GroupCountMagnitudeType implements GroupReduceFunction<Tuple3<Tuple2<Integer, Integer>, String, Integer>, Tuple3<Tuple2<Integer, Integer>, String, Integer>> {

        /**
         * @param values -> [(min_Magnitude, max_Magnitude), Magnitude Type, Count(always 1)] as Output
         * @param out    -> [(min_Magnitude, max_Magnitude), Magnitude Type, Count(always 1)] as Output
         */
        @Override
        public void reduce(Iterable<Tuple3<Tuple2<Integer, Integer>, String, Integer>> values, Collector<Tuple3<Tuple2<Integer, Integer>, String, Integer>> out) throws Exception {
            HashMap<Integer, HashMap<String, Integer>> minMagToTypeToCountMap = new HashMap<>();

            for (int minMagnitude : MAGNITUDES) {
                HashMap<String, Integer> typeToCount = new HashMap<>();
                for (MagnitudeType type : MagnitudeType.values()) {
                    typeToCount.put(type.name(), 0);
                }
                minMagToTypeToCountMap.put(minMagnitude, typeToCount);
            }
            values.iterator().forEachRemaining(value -> {
                HashMap<String, Integer> currentValues = minMagToTypeToCountMap.get(value.f0.f0);
                for (String magTypeName : currentValues.keySet()) {
                    for (String shortForm : MagnitudeType.valueOf(magTypeName).getShortForms()) {
                        if (shortForm.equalsIgnoreCase(value.f1)) {
                            currentValues.merge(magTypeName, 1, Integer::sum);
                            break;
                        }
                    }
                }
//                minMagToTypeToCountMap.merge(value.f0.f0, currentValues, (currentValue, newValue) -> {
//                    currentValue.putAll(newValue);
//                    return currentValue;
//                });
            });
            minMagToTypeToCountMap
                    .forEach((minMag, typeToCount) ->
                            typeToCount.forEach((type, count) ->
                                    out.collect(new Tuple3<>(new Tuple2<>(minMag, minMag + 1), type, count))));
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

    private static class PlaceSIGAndCoordinates implements FlatMapFunction<Feature, Tuple4<String, Double, Double, Long>> {

        // out -> (Location, latitude, longitude, SIG )
        @Override
        public void flatMap(Feature feature, Collector<Tuple4<String, Double, Double, Long>> collector) {
            List<Double> coordinates = feature.geometry.coordinates;
            if (!coordinates.isEmpty()) {
                collector.collect(new Tuple4<>(feature.properties.place, coordinates.get(1), coordinates.get(0), feature.properties.sig));
            }
        }
    }

    private static class PlaceSIGAndCountry implements FlatMapFunction<Tuple4<String, Double, Double, Long>, Tuple3<String, String, Long>> {

        private List<Location> locations = TransformEarthquakeJSON.readLocationsFromCSV(pathToLocations);

        private PlaceSIGAndCountry() throws IOException {
        }

        @Override
        public void flatMap(Tuple4<String, Double, Double, Long> value, Collector<Tuple3<String, String, Long>> collector) throws Exception {
            String country = getCountry(value.f1, value.f2);
            collector.collect(new Tuple3<>(value.f0, country, value.f3));
        }

        private String getCountry(double latitude, double longitude) throws Exception {
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

    private static class MapEventsToLocationCoordinates implements FlatMapFunction<Feature, Tuple5<Double, Double, Long, Double, Long>> {

        // out -> (latitude, longitude, SIG, nst, tsunami)
        @Override
        public void flatMap(Feature feature, Collector<Tuple5<Double, Double, Long, Double, Long>> collector) {
            List<Double> coordinates = feature.geometry.coordinates;
            if (!coordinates.isEmpty()) {
                collector.collect(new Tuple5<>(coordinates.get(1), coordinates.get(0), feature.properties.sig, feature.properties.nst != null ? feature.properties.nst : NULL_VALUE, feature.properties.tsunami));
            }
        }

        private Boolean getTsunami(Long tsunami) {
            if (tsunami != null) {
                return tsunami == 1;
            }
            return null;
        }
    }

    private static class MapEventsToLocation implements FlatMapFunction<Tuple5<Double, Double, Long, Double, Long>, Tuple4<String, Long, Double, Long>> {

        private List<Location> locations = TransformEarthquakeJSON.readLocationsFromCSV(pathToLocations);

        private MapEventsToLocation() throws IOException {
        }

        @Override
        public void flatMap(Tuple5<Double, Double, Long, Double, Long> value, Collector<Tuple4<String, Long, Double, Long>> collector) throws Exception {
            String country = getCountry(value.f0, value.f1);
            collector.collect(new Tuple4<>(country, value.f2, value.f3, value.f4));
        }

        private String getCountry(double latitude, double longitude) throws Exception {
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
