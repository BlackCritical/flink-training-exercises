package com.ververica.flinktraining.project.model;

import lombok.Data;

@Data
public class Metadata {

    public Long generated;
    public String url;
    public String title;
    public Long status;
    public String api;
    public Long count;

}
