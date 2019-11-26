package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.magnitude.CountHistogram;
import com.ververica.flinktraining.project.model.EarthquakeCollection;
import com.ververica.flinktraining.project.model.Feature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import static com.ververica.flinktraining.project.OtherEarthquakeBatchProjectExercise.UNDEFINED_MAGNITUDE;

/**
 * Parameters:
 * -input path-to-input-file
 */
public class EarthquakeBatchProjectExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", pathToBigEarthquakeData);

        // set up batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        EarthquakeCollection earthquake = TransformEarthquakeJSON.readEarthquakeFromJSON(input);

        DataSet<Feature> earthquakes = env.fromCollection(earthquake.features);
        System.out.println(earthquakes.count());

        SortPartitionOperator<Tuple3<Integer, Integer, Integer>> hist = earthquakes
            .flatMap(new MagnitudeHistogram())
            .groupBy(0, 1)
            .reduce(new CountHistogram())
            .sortPartition(0, Order.ASCENDING);

        hist.print();
    }

    private static class MagnitudeHistogram implements FlatMapFunction<Feature, Tuple3<Integer, Integer, Integer>> {

        // out -> (min, max, count)
        @Override
        public void flatMap(Feature value, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
            if (value != null && value.properties != null && value.properties.mag != null) {
                double mag = value.properties.mag;
                for (int i = -1; i < 10; i++) {
                    if (i <= mag && mag < i + 1) {
                        out.collect(new Tuple3<>(i, i + 1, 1));
                        return;
                    }
                }
            } else {
                System.out.println(value);
            }
            out.collect(new Tuple3<>(UNDEFINED_MAGNITUDE, UNDEFINED_MAGNITUDE, 1));
        }
    }
}
