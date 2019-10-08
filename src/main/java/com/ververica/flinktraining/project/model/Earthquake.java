package com.ververica.flinktraining.project.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Earthquake {

    public String type;
    public Metadata metadata;
    public List<Feature> features = new ArrayList<>();
    public List<Double> bbox = new ArrayList<>();

}
