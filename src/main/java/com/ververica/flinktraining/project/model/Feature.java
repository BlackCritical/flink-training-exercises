package com.ververica.flinktraining.project.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class Feature implements Serializable {

    public String type;
    public Properties properties;
    public Geometry geometry;
    public String id;

}
