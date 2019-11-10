package com.ververica.flinktraining.project.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Location implements Serializable {

    public String name;
    public double longitude;
    public double latitude;

}
