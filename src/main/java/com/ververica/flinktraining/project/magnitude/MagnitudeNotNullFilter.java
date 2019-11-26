package com.ververica.flinktraining.project.magnitude;

import com.ververica.flinktraining.project.model.Feature;
import org.apache.flink.api.common.functions.FilterFunction;

public class MagnitudeNotNullFilter implements FilterFunction<Feature> {
    @Override
    public boolean filter(Feature value) {
        return value.properties.mag != null;
    }
}
