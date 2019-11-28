package com.example.bigdatareader.model;

import lombok.Data;

@Data
public class Feature {

    public String type;
    public Properties properties;
    public Geometry geometry;
    public String id;

}
