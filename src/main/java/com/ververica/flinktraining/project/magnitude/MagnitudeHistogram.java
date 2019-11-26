package com.ververica.flinktraining.project.magnitude;

import com.ververica.flinktraining.project.model.Feature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import static com.ververica.flinktraining.project.OtherEarthquakeBatchProjectExercise.MAGNITUDES;
import static com.ververica.flinktraining.project.OtherEarthquakeBatchProjectExercise.UNDEFINED;

public class MagnitudeHistogram implements FlatMapFunction<Feature, Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> {

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
                return;
            }
        }
    }

    /**
     * @return 1 if review else 0
     */
    private int isReviewed(String reviewStatus) {
        return (reviewStatus != null) ? (reviewStatus.equalsIgnoreCase("reviewed") ? 1 : 0) : 0;
    }
}
