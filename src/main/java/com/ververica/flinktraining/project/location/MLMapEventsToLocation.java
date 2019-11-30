package com.ververica.flinktraining.project.location;

import com.ververica.flinktraining.project.TransformEarthquakeJSON;
import com.ververica.flinktraining.project.model.Location;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

import static com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase.pathToLocations;

public class MLMapEventsToLocation extends RichFlatMapFunction<Tuple4<Double, Double, Long, Long>, Tuple3<String, Long, Long>> {

    // The Location list will be read out of a small CSV File by every flink node once.
    private List<Location> locations = TransformEarthquakeJSON.readLocationsFromCSV(pathToLocations);

    // map MinMag To MagnitudeCount And ReviewedCount
    private MapState<String, Integer> mapCountryToCount;
    // count
    private ValueState<Integer> count;

    @Override
    public void open(Configuration config) {
        mapCountryToCount = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapCountryToCount", String.class, Integer.class));
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Integer.class));
    }

    public MLMapEventsToLocation() throws IOException {
    }

    /**
     * Map the longitude and latitude to the country with the
     * smallest euclidean distance to the given latitude and longitude.
     *
     * @param value     -> [latitude, longitude, SIG, tsunami(1/0)]
     * @param collector     -> [countryName, SIG, tsunami(1/0)]
     */
    @Override
    public void flatMap(Tuple4<Double, Double, Long, Long> value, Collector<Tuple3<String, Long, Long>> collector) throws Exception {
        String country = getCountry(value.f0, value.f1);
        count.update(count.value() != null ? count.value() + 1 : 1);
        System.out.println(String.format("Actual value #%d: %s", count.value(), country));
        machineLearningPrediction3(country);
        collector.collect(new Tuple3<>(country, value.f2, value.f3));
    }

    private String getCountry(double latitude, double longitude) {
        double minDistance = Double.MAX_VALUE;
        String minName = "";
        for (Location location : locations) {
            double distance = euclideanDistance(latitude, longitude, location.latitude, location.longitude);
            if (distance < minDistance) {
                minDistance = distance;
                minName = location.name;
            }
        }
        return minName;
    }

    private void machineLearningPrediction3(String country) throws Exception {
        Integer sum = mapCountryToCount.get(country);
        if (sum == null) {
            mapCountryToCount.put(country, 1);
        } else {
            sum += 1;
            mapCountryToCount.put(country, sum);
        }
        String nextCountryPrediction = getNextCountryPrediction();
        System.out.println(String.format("Prediction for value #%d: %s", count.value() + 1, nextCountryPrediction));
    }

    private String getNextCountryPrediction() throws Exception {
        String nextPrediction = "";
        int highest = 0;
        for (String key : mapCountryToCount.keys()) {
            Integer countryCount = mapCountryToCount.get(key);
            if (countryCount > highest) {
                nextPrediction = key;
                highest = countryCount;
            }
        }
        return nextPrediction;
    }


    private static double euclideanDistance(double p1x, double p1y, double p2x, double p2y) {
        return Math.sqrt(Math.pow(p1x - p2x, 2) + Math.pow(p1y - p2y, 2));
    }
}