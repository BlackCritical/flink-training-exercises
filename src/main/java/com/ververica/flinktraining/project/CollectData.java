package com.ververica.flinktraining.project;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.tuple.Tuple2;

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

    public static void main(String[] args) {
        String basePath = "C:/Users/leander.nachtmann/IdeaProjects/flink-training-leander/output/stream/magnitude_mag/";
        File output = new File(basePath + "output.csv");

        for (String fileName : Arrays.asList("1", "2", "3", "4")) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(basePath + fileName), StandardCharsets.UTF_8))) {
                br.readLine(); // Skip Headline
                String line = br.readLine();

                while (line != null && !line.equals("")) {
                    version_1(line);
                    line = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            FileUtils.writeStringToFile(output, prettyMapString(mapMinMagToMagAndRev), "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println(prettyMapString(mapMinMagToMagAndRev));
        }
    }

    private static void version_1(String line) {
        String[] values = line.split(";");

        String minMaxMagnitude = values[0];
        int minMagnitude = Integer.parseInt(minMaxMagnitude.substring(1, minMaxMagnitude.indexOf(',')));
        int magnitudeCount = Integer.parseInt(values[1]);
        int reviewedCount = Integer.parseInt(values[2]);

        Tuple2<Integer, Integer> sumValues = mapMinMagToMagAndRev.get(minMagnitude);
        if (sumValues == null) {
            mapMinMagToMagAndRev.put(minMagnitude, new Tuple2<>(magnitudeCount, reviewedCount));
        } else {
            sumValues.f0 += magnitudeCount;
            sumValues.f1 += reviewedCount;
            mapMinMagToMagAndRev.put(minMagnitude, sumValues);
//            System.out.println(prettyMapString(mapMinMagToMagAndRev));
        }
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
