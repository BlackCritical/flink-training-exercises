package com.ververica.flinktraining.project.location;

import com.ververica.flinktraining.project.model.Feature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.List;

public class MapEventsToLocationCoordinates implements FlatMapFunction<Feature, Tuple4<Double, Double, Long, Long>> {

    /**
     * @param feature       -> Earthquake Feature
     * @param collector     -> [latitude, longitude, SIG, tsunami(1/0)]
     */
    @Override
    public void flatMap(Feature feature, Collector<Tuple4<Double, Double, Long, Long>> collector) {
        List<Double> coordinates = feature.geometry.coordinates;
        if (!coordinates.isEmpty()) {
            collector.collect(new Tuple4<>(coordinates.get(1), coordinates.get(0), feature.properties.sig, feature.properties.tsunami));
        }
    }
}