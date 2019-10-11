package com.ververica.flinktraining.exercises.datastream_java.sources;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.model.Earthquake;
import org.apache.flink.streaming.api.functions.ListSourceContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import java.util.ArrayList;

public class EarthquakeSourceTest {

    @Test
    public void run() throws Exception {
        SourceFunction.SourceContext<Earthquake> s = new ListSourceContext<>(new ArrayList<>());
        EarthquakeSource es = new EarthquakeSource(ExerciseBase.pathToEarthquakeData);
        es.run(s);
    }
}
