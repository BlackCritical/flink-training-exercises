package com.ververica.flinktraining.project;

import com.google.gson.Gson;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.project.model.EarthquakeCollection;
import com.ververica.flinktraining.project.model.Feature;
import com.ververica.flinktraining.project.model.Location;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class TransformEarthquakeJSON {

    private static final Gson GSON = new Gson();

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToALLEarthquakeData);
        final String inputCSV = params.get("inputCSV", ExerciseBase.pathToLocations);

        EarthquakeCollection earthquake = readEarthquakeFromJSON(input);
//        List<Location> locations = readLocationsFromCSV(inputCSV);
//        List<Location> locationsUSA = readLocationsFromCSV(inputCSV_USA);
//        System.out.println(locationsUSA.size());
//        System.out.println(locations.size());


//        writeSubList(earthquake);
//        mergeSources(earthquake);
        printTimeRange(earthquake);
//        findAndRemoveDuplicates(earthquake);
        featurePropertyNullCount(earthquake);
    }

    /**
     * Can be used to see how many features contain a certain property.
     * If there are a lot of null values (like for example for the property "alert")
     * it is not suitable for this project to be analyzed.
     */
    private static void featurePropertyNullCount(EarthquakeCollection earthquake) {
        int count = 0;
        for (Feature feature : earthquake.features) {
            Long magType = feature.properties.tsunami;
            if (magType == null) {
                count++;
            }
        }
        System.out.println("count: " + count);
    }

    /**
     * Look through the DataSet to find and remove duplicated entries
     */
    private static void findAndRemoveDuplicates(EarthquakeCollection earthquake) {
        LinkedList<String> dupIds = new LinkedList<>();
        LinkedList<Integer> dupIndexs = new LinkedList<>();
        List<Feature> features = earthquake.features;
        for (int i = 520000; i < features.size(); i++) {
            String id = features.get(i).id;

            for (int j = i + 1; j < features.size(); j++) {
                if (features.get(j).id.equals(id)) {
                    System.out.println(String.format("Duplicate Found!\nID: %s\nIndex: %d", id, j));
                    dupIds.add(id);
                    dupIndexs.add(j);
                }
            }
            if (i % 10000 == 0) {
                System.out.println("Currently at: " + i);
            }
        }
        System.out.println(Arrays.toString(dupIds.toArray()));
        dupIndexs.forEach(index -> earthquake.features.remove(index.intValue()));
        writeToFile(earthquake, "earthquakeALL-2014-2019CLEAN.json");
    }

    /**
     * Merge different earthquake.json files
     */
    private static void mergeSources(EarthquakeCollection earthquake) throws IOException {
        final String input2 = ExerciseBase.pathToNewData;
        EarthquakeCollection newData = readEarthquakeFromJSON(input2);

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

    /**
     * Print the first and last date in a DataSet to check in which time range this DataSet
     * was generated.
     */
    private static void printTimeRange(EarthquakeCollection earthquake) {
        System.out.println("Begin:");
        System.out.println(new Date(earthquake.features.get(0).properties.time));
        System.out.println("End:");
        System.out.println(new Date(earthquake.features.get(earthquake.features.size() - 1).properties.time));
    }

    /**
     * Create smaller DataSets for faster testing
     */
    private static void writeSubList(EarthquakeCollection earthquake) {
        earthquake.features = earthquake.features.subList(0, 100000);
        writeToFile(earthquake, "earthquake-medium.json");
    }

    /**
     * Write Java Object to JSON
     */
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

    /**
     * Reads a list of countries with their longitude and latitude
     * from a CSV File into an ArrayList for mapping coordinates to countries inside the:
     * com.ververica.flinktraining.project.location.MapEventsToLocation
     */
    public static List<Location> readLocationsFromCSV(String inputCSV) throws IOException {
        System.out.println("READ!");
        ArrayList<Location> locations = new ArrayList<>();
        InputStream gzipStream = new GZIPInputStream(new FileInputStream(inputCSV));
        BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));
        Scanner sc = new Scanner(reader)
                .useDelimiter("([;\n])");

        while (sc.hasNext()) {
            String name = sc.next();
            if (!name.equals("Locations")) {
                double latitude = readTude(sc);
                double longitude = readTude(sc);

                locations.add(Location.builder()
                        .name(name)
                        .latitude(latitude)
                        .longitude(longitude).build());
            } else {
                sc.next();
                sc.next();
            }
        }
        return locations;
    }

    private static double readTude(Scanner sc) {
        String tudeStr = sc.next();
        int latIndex = tudeStr.indexOf('\'');
        tudeStr = latIndex > -1 ? tudeStr.substring(0, latIndex) : tudeStr;
        return Double.parseDouble(tudeStr);
    }
}
