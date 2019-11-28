package com.ververica.flinktraining.project.magnitude.stream;

import com.ververica.flinktraining.project.util.MagnitudeType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

import static com.ververica.flinktraining.project.EarthquakeBatchProjectExercise.MAGNITUDES;

public class WindowCountMagnitudeType extends ProcessWindowFunction<Tuple3<Tuple2<Integer, Integer>, String, Integer>, Tuple3<Tuple2<Integer, Integer>, String, Integer>, Tuple, GlobalWindow> {

    /**
     * @param values ->[(min_Magnitude, max_Magnitude), Magnitude Count(always 1), Magnitude Type, Reviewed Status Count (1 if reviewed else 0)] as Input
     * @param out    -> [(min_Magnitude, max_Magnitude), Magnitude Count, Review Status]
     */
    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple3<Tuple2<Integer, Integer>, String, Integer>> values, Collector<Tuple3<Tuple2<Integer, Integer>, String, Integer>> out) {
        HashMap<Integer, HashMap<String, Integer>> minMagToTypeToCountMap = new HashMap<>();

        for (int minMagnitude : MAGNITUDES) {
            HashMap<String, Integer> typeToCount = new HashMap<>();
            for (MagnitudeType type : MagnitudeType.values()) {
                typeToCount.put(type.name(), 0);  // init with count 0
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
        });
        // set the count of every magnitudeType for this every range  (for every combination of MagnitudeType and MagnitudeRange)
        minMagToTypeToCountMap
            .forEach((minMag, typeToCount) ->
                typeToCount.forEach((type, count) ->
                    out.collect(new Tuple3<>(new Tuple2<>(minMag, minMag + 1), type, count))));
    }
}
