package com.ververica.flinktraining.project.location;

import com.ververica.flinktraining.project.TransformEarthquakeJSON;
import com.ververica.flinktraining.project.model.Location;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

import static com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase.pathToLocations;

public class MapEventsToLocation implements FlatMapFunction<Tuple4<Double, Double, Long, Long>, Tuple3<String, Long, Long>> {

    // The Location list will be read out of a small CSV File by every flink node once.
    private List<Location> locations = TransformEarthquakeJSON.readLocationsFromCSV(pathToLocations);

    public MapEventsToLocation() throws IOException {
    }

    /**
     * Map the longitude and latitude to the country with the
     * smallest euclidean distance to the given latitude and longitude.
     *
     * @param value     -> [latitude, longitude, SIG, tsunami(1/0)]
     * @param collector     -> [countryName, SIG, tsunami(1/0)]
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
            double distance = euclideanDistance(latitude, longitude, location.latitude, location.longitude);
            if (distance < minDistance) {
                minDistance = distance;
                minName = location.name;
            }
        }
        return minName;
    }

    private static double euclideanDistance(double p1x, double p1y, double p2x, double p2y) {
        return Math.sqrt(Math.pow(p1x - p2x, 2) + Math.pow(p1y - p2y, 2));
    }
}