package com.ververica.flinktraining.project.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EarthquakeCollection {

    public String type;
    public Metadata metadata;
    public List<Feature> features = new LinkedList<>();
    public List<Double> bbox = new LinkedList<>();

    public List<Metadata> allMetadata = new LinkedList<>();

}
