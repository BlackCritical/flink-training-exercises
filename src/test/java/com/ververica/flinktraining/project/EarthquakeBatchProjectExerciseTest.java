package com.ververica.flinktraining.project;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.Locale;

import static org.junit.Assert.assertNotEquals;

public class EarthquakeBatchProjectExerciseTest {

    @Test
    public void main() {
        assertNotEquals(new Tuple2<>(1, 2), new Tuple2<>(5, 6));
    }

    @Test
    public void main2() {
        assertNotEquals(" ", String.format(Locale.US, "%s;%d;%f;%s;", "value.f0, value.f1, value.f2, value.f3", 34, 0.123, new Boolean(true)));
    }
}
