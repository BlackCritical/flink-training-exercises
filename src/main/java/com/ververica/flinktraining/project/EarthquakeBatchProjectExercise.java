package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.model.EarthquakeCollection;
import com.ververica.flinktraining.project.model.Feature;
import com.ververica.flinktraining.project.model.Geometry;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static com.ververica.flinktraining.project.OtherEarthquakeBatchProjectExercise.UNDEFINED_MAGNITUDE;

/**
 * The "Ride Cleansing" exercise from the Flink training
 * (http://training.ververica.com).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 * <p>
 * Parameters:
 * -input path-to-input-file
 */
public class EarthquakeBatchProjectExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", pathToEarthquakeData);

        // set up batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        EarthquakeCollection earthquake = TransformEarthquakeJSON.readEarthquakeFromJSON(input);

        DataSet<Feature> earthquakes = env.fromCollection(earthquake.features);
        System.out.println(earthquakes.count());

        SortPartitionOperator<Tuple3<Integer, Integer, Integer>> hist = earthquakes
            .flatMap(new MagnitudeHistogram())
            .groupBy(1, 2)
            .reduce(new CountHistogram())
            .sortPartition(1, Order.ASCENDING);

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
            out.collect(new Tuple3<>(UNDEFINED_MAGNITUDE, UNDEFINED_MAGNITUDE, 1));
        }
    }

    public static class CountHistogram implements ReduceFunction<Tuple3<Integer, Integer, Integer>> {
        @Override
        public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> firstTuple, Tuple3<Integer, Integer, Integer> secondTuple) throws Exception {
            return new Tuple3<>(firstTuple.f0, firstTuple.f1, firstTuple.f2 + secondTuple.f2);
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

    private static class CountAssigner implements FlatMapFunction<Feature, Tuple2<Feature, Integer>> {
        @Override
        public void flatMap(Feature value, Collector<Tuple2<Feature, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value, 1));
        }
    }

    public static class CountReducer implements ReduceFunction<Tuple2<Feature, Integer>> {
        @Override
        public Tuple2<Feature, Integer> reduce(Tuple2<Feature, Integer> firstTuple, Tuple2<Feature, Integer> secondTuple) throws Exception {
            return new Tuple2<>(firstTuple.f0, firstTuple.f1 + secondTuple.f1);
        }
    }

    private static class ReduceGroup implements GroupReduceFunction<Tuple2<Feature, Integer>, Tuple2<Feature, Integer>> {
        @Override
        public void reduce(Iterable<Tuple2<Feature, Integer>> values, Collector<Tuple2<Feature, Integer>> out) throws Exception {
            List<Tuple2<Feature, Integer>> list = new ArrayList<>();
            values.iterator().forEachRemaining(list::add);
            out.collect(new Tuple2<>(list.get(0).f0, list.size()));
        }
    }
}
