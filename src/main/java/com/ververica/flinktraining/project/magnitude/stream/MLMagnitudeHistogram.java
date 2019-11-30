package com.ververica.flinktraining.project.magnitude.stream;

import com.ververica.flinktraining.project.model.Feature;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.IOException;

import static com.ververica.flinktraining.project.EarthquakeBatchProjectExercise.MAGNITUDES;
import static com.ververica.flinktraining.project.EarthquakeBatchProjectExercise.UNDEFINED;

public class MLMagnitudeHistogram extends RichFlatMapFunction<Feature, Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> {

    // average
    private ValueState<Double> average;
    // count
    private ValueState<Integer> count;

    @Override
    public void open(Configuration config) {
        average = getRuntimeContext().getState(new ValueStateDescriptor<>("average", Double.class));
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Integer.class));
    }


    /**
     * @param value Feature as Input
     * @param out   -> [(min_Magnitude, max_Magnitude), Magnitude Count(always 1), Magnitude Type, Reviewed Status Count (1 if reviewed else 0)]
     */
    @Override
    public void flatMap(Feature value, Collector<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> out) throws IOException {
        String magType = value.properties.magType;
        String reviewStatus = value.properties.status;
        double mag = value.properties.mag;
        count.update(count.value() != null ? count.value() + 1 : 1);
        System.out.println(String.format("Actual value #%d: %f", count.value(), mag));
        machineLearningPrediction(mag);

        for (int minMagnitude : MAGNITUDES) {
            if (minMagnitude <= mag && mag < minMagnitude + 1) {  // find correct range
                magType = magType != null ? magType : UNDEFINED;
                out.collect(new Tuple4<>(new Tuple2<>(minMagnitude, minMagnitude + 1), 1, magType, isReviewed(reviewStatus)));
                return;
            }
        }
    }

    private void machineLearningPrediction(double mag) throws IOException {
        Double currentAverage = average.value();
        if (currentAverage == null) {
            average.update(mag);
        } else {
            double currentCount = count.value();
            double nextAverage = (currentCount - 1) / currentCount * currentAverage + (1 / currentCount) * mag;

            average.update(nextAverage);
        }
        System.out.println(String.format("Prediction for value #%d: %f", count.value() + 1, average.value()));
    }

    /**
     * @return 1 if review else 0
     */
    private int isReviewed(String reviewStatus) {
        return (reviewStatus != null) ? (reviewStatus.equalsIgnoreCase("reviewed") ? 1 : 0) : 0;
    }
}
