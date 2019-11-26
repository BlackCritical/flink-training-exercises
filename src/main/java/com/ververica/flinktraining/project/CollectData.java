package com.ververica.flinktraining.project;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@SuppressWarnings("Duplicates")
public class CollectData {

    // map MinMag To MagnitudeCount And ReviewedCount
    private static HashMap<Integer, Tuple2<Integer, Integer>> mapMinMagToMagAndRev = new HashMap<>();
    private static HashMap<String, Tuple3<Integer, String, Integer>> mapMinMagToTypeAndMag = new HashMap<>();

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        String basePath = "C:/Users/lvlup/IdeaProjects/flink-training-exercises/output/stream/magnitude_type/";
        File output = new File(basePath + "output.csv");
        HashMap chosenMap = mapMinMagToTypeAndMag;

        for (String fileName : Arrays.asList("1", "2", "3", "4")) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(basePath + fileName), StandardCharsets.UTF_8))) {
                String line = br.readLine();

                while (line != null && !line.equals("")) {
                    version_2(line);
                    line = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            FileUtils.writeStringToFile(output, generateCSV_2(chosenMap), "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println(prettyMapString(chosenMap));
        }
    }

    private static String generateCSV(HashMap<Integer, Tuple2<Integer, Integer>> map) {
        StringBuilder b = new StringBuilder();
        Object[] keys = map.keySet().toArray();
        Arrays.sort(keys);
        for (Object minMagnitude : keys) {
            int minMag = (int) minMagnitude;
            Tuple2<Integer, Integer> sums = map.get(minMag);
            b.append(minMag)
                    .append(";")
                    .append(minMag + 1)
                    .append(";")
                    .append(sums.f0)
                    .append(";")
                    .append(sums.f1)
                    .append("\n");
        }
        return b.toString();
    }

    private static <T> String generateCSV_2(HashMap<String, Tuple3<Integer, String, Integer>> map) {
        StringBuilder b = new StringBuilder();
        Object[] keys = map.keySet().toArray();
        Arrays.sort(keys);
        for (Object minMagnitude : keys) {
            Tuple3<Integer, String, Integer> sums = map.get(minMagnitude);
            int minMag = sums.f0;
            b.append(minMag)
                    .append(";")
                    .append(minMag + 1)
                    .append(";")
                    .append(sums.f1)
                    .append(";")
                    .append(sums.f2)
                    .append("\n");
        }
        return b.toString();
    }

    private static void version_1(String line) {
        String[] values = line.split(";");

        String minMaxMagnitude = values[0];
        int minMagnitude = Integer.parseInt(minMaxMagnitude.substring(1, minMaxMagnitude.indexOf(',')));
        int magnitudeCount = Integer.parseInt(values[1]);
        int reviewedCount = Integer.parseInt(values[2]);

        mapMinMagToMagAndRev.put(minMagnitude, new Tuple2<>(magnitudeCount, reviewedCount));  // Always override old values because we are only interested in the last values
    }

    private static void version_2(String line) {
        String[] values = line.split(";");

        String minMaxMagnitude = values[0];
        int minMagnitude = Integer.parseInt(minMaxMagnitude.substring(1, minMaxMagnitude.indexOf(',')));
        String magnitudeType = values[1];
        int magnitudeCount = Integer.parseInt(values[2]);

        mapMinMagToTypeAndMag.put(minMagnitude + magnitudeType, new Tuple3<>(minMagnitude, magnitudeType, magnitudeCount));  // Always override old values because we are only interested in the last values
    }

    public static <K, V> String prettyMapString(Map<K, V> map) {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<K, V> entry = iter.next();
            sb.append(entry.getKey());
            sb.append('=').append('"');
            sb.append(entry.getValue());
            sb.append('"');
            if (iter.hasNext()) {
                sb.append(',').append(' ');
            }
        }
        return sb.toString();
    }
}
