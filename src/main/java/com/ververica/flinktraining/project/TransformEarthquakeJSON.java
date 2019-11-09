package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.model.Earthquake;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import static com.ververica.flinktraining.exercises.datastream_java.sources.EarthquakeSource.GSON;


public class TransformEarthquakeJSON {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToEarthquakeData);

        Earthquake earthquake = readEarthquakeFromJSON(input);
//        writeSubList(earthquake);
        printTimeRange(earthquake);
    }

    private static void printTimeRange(Earthquake earthquake) {
        System.out.println("Begin:");
        System.out.println(new Date(earthquake.features.get(0).properties.time));
        System.out.println("End:");
        System.out.println(new Date(earthquake.features.get(earthquake.features.size() - 1).properties.time));
    }

    private static void writeSubList(Earthquake earthquake) {
        earthquake.features = earthquake.features.subList(0, 100000);
        try (PrintWriter out = new PrintWriter("earthquake-medium.json")) {
            out.println(GSON.toJson(earthquake));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Earthquake readEarthquakeFromJSON(String path) throws IOException {
        InputStream gzipStream = new GZIPInputStream(new FileInputStream(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));

        return GSON.fromJson(reader, Earthquake.class);
    }
}
