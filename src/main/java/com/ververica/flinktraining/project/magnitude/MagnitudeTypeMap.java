package com.ververica.flinktraining.project.magnitude;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class MagnitudeTypeMap implements FlatMapFunction<Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer>, Tuple3<Tuple2<Integer, Integer>, String, Integer>> {

    /**
     * @param value -> [(min_Magnitude, max_Magnitude), Magnitude Count(always 1), Magnitude Type, Reviewed Status Count (1 if reviewed else 0)] as Input
     * @param out   -> [(min_Magnitude, max_Magnitude), Magnitude Type, Count(always 1)] as Output
     */
    @Override
    public void flatMap(Tuple4<Tuple2<Integer, Integer>, Integer, String, Integer> value, Collector<Tuple3<Tuple2<Integer, Integer>, String, Integer>> out) throws Exception {
        out.collect(new Tuple3<>(value.f0, value.f2, 1));
    }
}
