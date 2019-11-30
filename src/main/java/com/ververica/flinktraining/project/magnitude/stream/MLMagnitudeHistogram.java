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
    private ValueState<Double> averageMagnitude;
    // reviewed count
    private ValueState<Integer> reviewedCount;
    // count
    private ValueState<Integer> count;

    @Override
    public void open(Configuration config) {
        averageMagnitude = getRuntimeContext().getState(new ValueStateDescriptor<>("averageMagnitude", Double.class));
        reviewedCount = getRuntimeContext().getState(new ValueStateDescriptor<>("reviewedLikelihood", Integer.class));
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Integer.class));
    }

    /**
     * @param value Feature as Input
     * @param out   -> [(min_Magnitude, max_Magnitude), Magnitude Count(always 1), Magnitude Type, Reviewed Status Count (1 if reviewed else 0)]
     */
    @Override
    public void flatMap(Feature value, Collector<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> out) throws IOException {
        String magType = value.properties.magType;
        double mag = value.properties.mag;
        long tsunami = value.properties.tsunami;
        int reviewed = isReviewed(value.properties.status);

        count.update(count.value() != null ? count.value() + 1 : 1);
        System.out.println(String.format("Actual value %s Tsunami #%d: %f", getTsunamiText(tsunami), count.value(), mag));
        if (reviewed == 1) {
            System.out.println(String.format("Actual value %s Tsunami #%d was Reviewed.", getTsunamiText(tsunami), count.value()));
        } else {
            System.out.println(String.format("Actual value %s Tsunami #%d was NOT Reviewed.", getTsunamiText(tsunami), count.value()));
        }

        machineLearningPrediction1(mag, tsunami);
        machineLearningPrediction2(reviewed, tsunami);

        for (int minMagnitude : MAGNITUDES) {
            if (minMagnitude <= mag && mag < minMagnitude + 1) {  // find correct range
                magType = magType != null ? magType : UNDEFINED;
                out.collect(new Tuple4<>(new Tuple2<>(minMagnitude, minMagnitude + 1), 1, magType, reviewed));
                return;
            }
        }
    }

    /**
     * Calculate the likelihood for the next earthquake feature
     * to be reviewed.
     *
     * Since this is a RichFlatMapFunction on a keyed stream, the reviewedCount exists for every
     * key group. This stream is keyed on the Tsunami property. That means, there are only two key groups
     * one with a tsunami's and one without tsunami's!
     *
     * @param reviewStatus 1 if reviewed 0 otherwise
     * @param tsunami 1 if a tsunami happened 0 otherwise
     */
    private void machineLearningPrediction2(int reviewStatus, long tsunami) throws IOException {
        Integer currentLikelihood = reviewedCount.value();
        int currentReviewedCount;
        if (currentLikelihood == null) {
            currentReviewedCount = reviewStatus;
        } else {
            currentReviewedCount = reviewedCount.value() + reviewStatus;
        }
        reviewedCount.update(currentReviewedCount);
        double currentCount = count.value();
        double nextLikelihood = currentReviewedCount / currentCount;
        System.out.println(String.format("Prediction: Value %s Tsunami #%d will be reviewed with a likelihood of: %f", getTsunamiText(tsunami), count.value() + 1, nextLikelihood));
    }

    /**
     * Calculates incrementally the average for all magnitudes processed so far.
     * The average value will be used as prediction.
     *
     * Since this is a RichFlatMapFunction on a keyed stream, the averageMagnitude exists for every
     * key group. This stream is keyed on the Tsunami property. That means, there are only two key groups
     * one with a tsunami's and one without tsunami's!
     *
     * @param mag the magnitude of the earthquake feature
     * @param tsunami 1 if a tsunami happened 0 otherwise
     */
    private void machineLearningPrediction1(double mag, long tsunami) throws IOException {
        Double currentAverage = averageMagnitude.value();
        if (currentAverage == null) {
            averageMagnitude.update(mag);
        } else {
            double currentCount = count.value();
            double nextAverage = (currentCount - 1) / currentCount * currentAverage + (1 / currentCount) * mag;

            averageMagnitude.update(nextAverage);
        }
        System.out.println(String.format("Prediction for value %s Tsunami #%d: %f", getTsunamiText(tsunami), count.value() + 1, averageMagnitude.value()));
    }

    private String getTsunamiText(long tsunami) {
        return tsunami == 1 ? "WITH" : "WITHOUT";
    }

    /**
     * @return 1 if review else 0
     */
    private int isReviewed(String reviewStatus) {
        return (reviewStatus != null) ? (reviewStatus.equalsIgnoreCase("reviewed") ? 1 : 0) : 0;
    }
}
