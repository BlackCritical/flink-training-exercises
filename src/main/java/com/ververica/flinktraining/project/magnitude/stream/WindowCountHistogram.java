package com.ververica.flinktraining.project.magnitude.stream;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

import static com.ververica.flinktraining.project.OtherEarthquakeBatchProjectExercise.MAGNITUDES;

public class WindowCountHistogram extends ProcessWindowFunction<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>, Tuple3<Tuple2<Integer, Integer>, Integer, Integer>, Tuple, GlobalWindow> {

    /**
     * @param values ->[(min_Magnitude, max_Magnitude), Magnitude Count(always 1), Magnitude Type, Reviewed Status Count (1 if reviewed else 0)] as Input
     * @param out    -> [(min_Magnitude, max_Magnitude), Magnitude Count, Review Status]
     */
    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>> values, Collector<Tuple3<Tuple2<Integer, Integer>, Integer, Integer>> out) throws Exception {
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
