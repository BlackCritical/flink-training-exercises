package com.ververica.flinktraining.project.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class Geometry {

    public String type;
    public List<Double> coordinates = new ArrayList<>();

}
