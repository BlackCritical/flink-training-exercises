package com.ververica.flinktraining.project.magnitude;

import com.ververica.flinktraining.project.util.MagnitudeType;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.HashMap;

import static com.ververica.flinktraining.project.OtherEarthquakeBatchProjectExercise.MAGNITUDES;

public class GroupCountMagnitudeType implements GroupReduceFunction<Tuple3<Tuple2<Integer, Integer>, String, Integer>, Tuple3<Tuple2<Integer, Integer>, String, Integer>> {

    /**
     * @param values -> [(min_Magnitude, max_Magnitude), Magnitude Type, Count(always 1)] as Output
     * @param out    -> [(min_Magnitude, max_Magnitude), Magnitude Type, Count(always 1)] as Output
     */
    @Override
    public void reduce(Iterable<Tuple3<Tuple2<Integer, Integer>, String, Integer>> values, Collector<Tuple3<Tuple2<Integer, Integer>, String, Integer>> out) throws Exception {
        HashMap<Integer, HashMap<String, Integer>> minMagToTypeToCountMap = new HashMap<>();

        for (int minMagnitude : MAGNITUDES) {
            HashMap<String, Integer> typeToCount = new HashMap<>();
            for (MagnitudeType type : MagnitudeType.values()) {
                typeToCount.put(type.name(), 0);
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
//                minMagToTypeToCountMap.merge(value.f0.f0, currentValues, (currentValue, newValue) -> {
//                    currentValue.putAll(newValue);
//                    return currentValue;
//                });
        });
        minMagToTypeToCountMap
            .forEach((minMag, typeToCount) ->
                typeToCount.forEach((type, count) ->
                    out.collect(new Tuple3<>(new Tuple2<>(minMag, minMag + 1), type, count))));
    }
}
