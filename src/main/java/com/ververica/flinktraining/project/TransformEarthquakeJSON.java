package com.ververica.flinktraining.project;

import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.model.EarthquakeCollection;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.ververica.flinktraining.exercises.datastream_java.sources.EarthquakeSource.GSON;


public class TransformEarthquakeJSON {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToBigEarthquakeData);
        final String input2 = params.get("input2", ExerciseBase.pathToNewData);

        EarthquakeCollection earthquake = readEarthquakeFromJSON(input);
        EarthquakeCollection newData = readEarthquakeFromJSON(input2);
//        writeSubList(earthquake);
        mergeSources(earthquake, newData);
        printTimeRange(earthquake);
    }

    private static void mergeSources(EarthquakeCollection earthquake, EarthquakeCollection newData) {
        earthquake.features.addAll(newData.features);
        earthquake.allMetadata.add(newData.metadata);

        List<Double> doubles = newData.bbox;
        List<Double> resultBbox = earthquake.bbox;
        for (int i = 0; i < 3; i++) {
            Double bbox = doubles.get(i);
            resultBbox.set(i, resultBbox.get(i) < bbox ? resultBbox.get(i) : bbox);
        }
        for (int i = 3; i < 6; i++) {
            Double bbox = doubles.get(i);
            resultBbox.set(i, resultBbox.get(i) > bbox ? resultBbox.get(i) : bbox);
        }
        writeToFile(earthquake, "earthquake2014-2019.json");
    }

    private static void printTimeRange(EarthquakeCollection earthquake) {
        System.out.println("Begin:");
        System.out.println(new Date(earthquake.features.get(0).properties.time));
        System.out.println("End:");
        System.out.println(new Date(earthquake.features.get(earthquake.features.size() - 1).properties.time));
    }

    private static void writeSubList(EarthquakeCollection earthquake) {
        earthquake.features = earthquake.features.subList(0, 100000);
        writeToFile(earthquake, "earthquake-medium.json");
    }

    private static void writeToFile(EarthquakeCollection earthquake, String fileName) {
        try (PrintWriter out = new PrintWriter(fileName)) {
            out.println(GSON.toJson(earthquake));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static EarthquakeCollection readEarthquakeFromJSON(String path) throws IOException {
        InputStream gzipStream = new GZIPInputStream(new FileInputStream(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));

        return GSON.fromJson(reader, EarthquakeCollection.class);
    }
}
