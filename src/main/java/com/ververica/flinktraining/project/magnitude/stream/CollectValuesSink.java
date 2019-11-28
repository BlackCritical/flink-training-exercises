package com.ververica.flinktraining.project.magnitude.stream;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;


// Unused
public class CollectValuesSink extends RichSinkFunction<Tuple3<Tuple2<Integer, Integer>, Integer, Integer>> {

    // map MinMag To MagnitudeCount And ReviewedCount
    private MapState<Integer, Tuple2<Integer, Integer>> mapMinMagToMagAndRev;

    @Override
    public void open(Configuration config) throws Exception {
        TupleTypeInfo<Tuple2<Integer, Integer>> tupleType = new TupleTypeInfo<>();
        mapMinMagToMagAndRev = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapMinMagToMagAndRev", INT_TYPE_INFO , tupleType));
    }

    @Override
    public void invoke(Tuple3<Tuple2<Integer, Integer>, Integer, Integer> record, Context context) throws Exception {
        Tuple2<Integer, Integer> sumValues = mapMinMagToMagAndRev.get(record.f0.f0);
        if (sumValues == null) {
            mapMinMagToMagAndRev.put(record.f0.f0, new Tuple2<>(record.f1, record.f2));
        } else {
            sumValues.f0 += record.f1;
            sumValues.f1 += record.f2;
            mapMinMagToMagAndRev.put(record.f0.f0, sumValues);
            System.out.println(prettyMapString(mapMinMagToMagAndRev));
        }
    }

    @Override
    public void close() throws Exception {
        System.out.println("close");
        System.out.println(prettyMapString(mapMinMagToMagAndRev));
    }

    private static <K, V> String prettyMapString(MapState<K, V> map) throws Exception {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<K, V>> iter = map.entries().iterator();
        while (iter.hasNext()) {
            Map.Entry<K, V> entry = iter.next();
            sb.append(entry.getKey());
            sb.append('=').append('"');
            sb.append(entry.getValue());
            sb.append('"');
            if (iter.hasNext()) {
                sb.append(',').append(' ');
            }
        }
        return sb.toString();
    }
}
