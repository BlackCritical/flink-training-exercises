package com.ververica.flinktraining.project.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Location {

    public String name;
    public double minLongitude;
    public double maxLongitude;
    public double minLatitude;
    public double maxLatitude;

}
