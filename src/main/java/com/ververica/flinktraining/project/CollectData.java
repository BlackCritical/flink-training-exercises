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

    // map MinMagnitude To MagnitudeCount And ReviewedCount
    private static HashMap<Integer, Tuple2<Integer, Integer>> mapMinMagToMagAndRev = new HashMap<>();

    // map concat(MinMagnitude,MagnitudeType) To minMag, MagnitudeType and MagnitudeCount
    private static HashMap<String, Tuple3<Integer, String, Integer>> mapMinMagTypeToTypeAndMag = new HashMap<>();

    // map Country Name To MaxSIG and MaxTsunami
    private static HashMap<String, Tuple2<Integer, Integer>> mapCountryToSIGAndTsunami = new HashMap<>();


    /**
     * Use this Mainclass to combine the different results from every node used for stream processing inside:
     * com.ververica.flinktraining.project.EarthquakeStreamProjectExercise
     */
    public static void main(String[] args) {
        String basePath = "./output/stream/magnitude_mag/";
        File output = new File(basePath + "output.csv");

        run(basePath, output, mapMinMagToMagAndRev, 0);

        basePath = "./output/stream/magnitude_rev/";
        output = new File(basePath + "output.csv");

        run(basePath, output, mapMinMagToMagAndRev, 1);

        basePath = "./output/stream/magnitude_type/";
        output = new File(basePath + "output.csv");

        run(basePath, output, mapMinMagTypeToTypeAndMag, 2);

        basePath = "./output/stream/max-sig-location/";
        output = new File(basePath + "output.csv");

        run(basePath, output, mapCountryToSIGAndTsunami, 3);

        basePath = "./output/stream/sum-Tsunami-location/";
        output = new File(basePath + "output.csv");

        run(basePath, output, mapCountryToSIGAndTsunami, 4);
    }

    private static void run(String basePath, File output, HashMap map, int index) {
        for (String fileName : Arrays.asList("1", "2", "3", "4")) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(basePath + fileName), StandardCharsets.UTF_8))) {
                String line = br.readLine();

                while (line != null && !line.equals("")) {
                    if (index == 0 || index == 1) {
                        version_1(line);
                    } else if (index == 2) {
                        version_2(line);
                    } else if (index > 2) {
                        version_3(line);
                    }
                    line = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {

            if (index == 0) {
                FileUtils.writeStringToFile(output, generateCSV_1("first"), "UTF-8");
            } else if (index == 1) {
                FileUtils.writeStringToFile(output, generateCSV_1("second"), "UTF-8");
            } else if (index == 2) {
                FileUtils.writeStringToFile(output, generateCSV_2(), "UTF-8");
            } else if (index == 3) {
                FileUtils.writeStringToFile(output, generateCSV_3("first"), "UTF-8");
            } else if (index == 4) {
                FileUtils.writeStringToFile(output, generateCSV_3("second"), "UTF-8");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println(prettyMapString(map));
        }
    }

    private static String generateCSV_1(String first) {
        StringBuilder b = new StringBuilder();
        Object[] keys = mapMinMagToMagAndRev.keySet().toArray();
        Arrays.sort(keys);
        for (Object minMagnitude : keys) {
            int minMag = (int) minMagnitude;
            Tuple2<Integer, Integer> sums = mapMinMagToMagAndRev.get(minMag);
            b.append(minMag)
                .append(";")
                .append(minMag + 1)
                .append(";");

            b.append(first.equals("first") ? sums.f0 : sums.f1);
            b.append("\n");
        }
        return b.toString();
    }

    private static String generateCSV_2() {
        StringBuilder b = new StringBuilder();
        Object[] keys = mapMinMagTypeToTypeAndMag.keySet().toArray();
        Arrays.sort(keys);
        for (Object minMagnitudeAndMagType : keys) {
            Tuple3<Integer, String, Integer> sums = mapMinMagTypeToTypeAndMag.get(minMagnitudeAndMagType);
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

    private static String generateCSV_3(String first) {
        StringBuilder b = new StringBuilder();
        Object[] keys = mapCountryToSIGAndTsunami.keySet().toArray();
        Arrays.sort(keys);
        for (Object countryKey : keys) {
            String country = (String) countryKey;
            Tuple2<Integer, Integer> sums = mapCountryToSIGAndTsunami.get(country);
            b.append(country)
                .append(";");

            b.append(first.equals("first") ? sums.f0 : sums.f1);
            b.append("\n");
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

        Tuple3<Integer, String, Integer> tuple3 = mapMinMagTypeToTypeAndMag.get(minMagnitude + magnitudeType);

        if (tuple3 != null) {
            magnitudeCount = tuple3.f2 > magnitudeCount ? tuple3.f2 : magnitudeCount;  // Always override old values because we are only interested in the last values
        }
        mapMinMagTypeToTypeAndMag.put(minMagnitude + magnitudeType, new Tuple3<>(minMagnitude, magnitudeType, magnitudeCount));
    }

    private static void version_3(String line) {
        String[] values = line.split(";");

        String countryName = values[0];
        int maxSig = Integer.parseInt(values[1]);
        int tsunamiSum = Integer.parseInt(values[2]);

        mapCountryToSIGAndTsunami.put(countryName, new Tuple2<>(maxSig, tsunamiSum));  // Always override old values because we are only interested in the last values
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
