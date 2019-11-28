package com.example.bigdatareader.model;

import lombok.Data;

import java.util.LinkedList;
import java.util.List;

@Data
public class Geometry {

    public String type;
    public List<Double> coordinates = new LinkedList<>();

}
