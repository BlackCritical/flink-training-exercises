package com.ververica.flinktraining.project.magnitude.stream;

import com.ververica.flinktraining.project.util.MagnitudeType;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

import static com.ververica.flinktraining.project.EarthquakeBatchProjectExercise.MAGNITUDES;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;

public class WindowCountMagnitudeType extends ProcessWindowFunction<Tuple3<Tuple2<Integer, Integer>, String, Integer>, Tuple3<Tuple2<Integer, Integer>, String, Integer>, Tuple, GlobalWindow> {

    // map MinMag To MagnitudeCount And ReviewedCount
    private MapState<Integer, Map<String, Integer>> minMagToTypeToCountMap;

    @Override
    public void open(Configuration config) {
        MapTypeInfo<String, Integer> mapTypeInfo = new MapTypeInfo<>(String.class, Integer.class);
        minMagToTypeToCountMap = getRuntimeContext().getMapState(new MapStateDescriptor<>("minMagToTypeToCountMap", INT_TYPE_INFO, mapTypeInfo));
    }

    /**
     * @param values ->[(min_Magnitude, max_Magnitude), Magnitude Count(always 1), Magnitude Type, Reviewed Status Count (1 if reviewed else 0)] as Input
     * @param out    -> [(min_Magnitude, max_Magnitude), Magnitude Count, Review Status]
     */
    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple3<Tuple2<Integer, Integer>, String, Integer>> values, Collector<Tuple3<Tuple2<Integer, Integer>, String, Integer>> out) throws Exception {
        if (!minMagToTypeToCountMap.keys().iterator().hasNext()) {
            for (int minMagnitude : MAGNITUDES) {
                HashMap<String, Integer> typeToCount = new HashMap<>();
                for (MagnitudeType type : MagnitudeType.values()) {
                    typeToCount.put(type.name(), 0);  // init with count 0
                }
                minMagToTypeToCountMap.put(minMagnitude, typeToCount);
            }
        }
        for (Tuple3<Tuple2<Integer, Integer>, String, Integer> value : values) {
            Map<String, Integer> currentValues = minMagToTypeToCountMap.get(value.f0.f0);
            for (String magTypeName : currentValues.keySet()) {
                for (String shortForm : MagnitudeType.valueOf(magTypeName).getShortForms()) {
                    if (shortForm.equalsIgnoreCase(value.f1)) {
                        currentValues.merge(magTypeName, 1, Integer::sum);
                        break;
                    }
                }
            }
        }
        // set the count of every magnitudeType for this every range  (for every combination of MagnitudeType and MagnitudeRange)
        minMagToTypeToCountMap.entries()
            .forEach(entry -> {
                Map<String, Integer> typeToCount = entry.getValue();
                Integer minMag = entry.getKey();
                typeToCount.forEach((type, count) ->
                    out.collect(new Tuple3<>(new Tuple2<>(minMag, minMag + 1), type, count)));
            });
    }
}
