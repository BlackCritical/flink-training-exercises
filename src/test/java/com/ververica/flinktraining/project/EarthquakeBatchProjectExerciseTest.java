package com.ververica.flinktraining.project;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

public class EarthquakeBatchProjectExerciseTest {

    @Test
    public void main() {
        assertNotEquals(new Tuple2<>(1, 2), new Tuple2<>(5,6));
    }
}
