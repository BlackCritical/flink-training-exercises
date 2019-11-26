package com.ververica.flinktraining.project.magnitude;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CountHistogram implements ReduceFunction<Tuple3<Integer, Integer, Integer>> {
    @Override
    public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> firstTuple, Tuple3<Integer, Integer, Integer> secondTuple) throws Exception {
        return new Tuple3<>(firstTuple.f0, firstTuple.f1, firstTuple.f2 + secondTuple.f2);
    }
}
