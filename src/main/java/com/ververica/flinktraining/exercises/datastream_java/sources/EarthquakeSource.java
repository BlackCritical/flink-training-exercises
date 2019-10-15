/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.sources;

import com.google.gson.Gson;
import com.ververica.flinktraining.project.model.Earthquake;
import com.ververica.flinktraining.project.model.Feature;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * This SourceFunction generates a data stream of Earthquake records which are
 * read from a gzipped input file. Each record has a time stamp and the input file must be
 * ordered by this time stamp.
 * <p>
 * In order to simulate a realistic stream source, the SourceFunction serves events proportional to
 * their timestamps. In addition, the serving of events can be delayed by a bounded random delay
 * which causes the events to be served slightly out-of-order of their timestamps.
 * <p>
 * The serving speed of the SourceFunction can be adjusted by a serving speed factor.
 * A factor of 60.0 increases the logical serving time by a factor of 60, i.e., events of one
 * minute (60 seconds) are served in 1 second.
 * <p>
 * This SourceFunction is an EventSourceFunction and does continuously emit watermarks.
 * Hence it is able to operate in event time mode which is configured as follows:
 * <p>
 * StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 */
public class EarthquakeSource implements SourceFunction<Feature> {

    private static final Gson GSON = new Gson();

    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final String dataFilePath;
    private final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;

    /**
     * Serves the Earthquake records from the specified and ordered gzipped input file.
     * Rides are served exactly in order of their time stamps
     * at the speed at which they were originally generated.
     *
     * @param dataFilePath The gzipped input file from which the Earthquake records are read.
     */
    public EarthquakeSource(String dataFilePath) {
        this(dataFilePath, 0, 1);
    }

    /**
     * Serves the Earthquake records from the specified and ordered gzipped input file.
     * Rides are served exactly in order of their time stamps
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath       The gzipped input file from which the Earthquake records are read.
     * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
     */
    public EarthquakeSource(String dataFilePath, int servingSpeedFactor) {
        this(dataFilePath, 0, servingSpeedFactor);
    }

    /**
     * Serves the Earthquake records from the specified and ordered gzipped input file.
     * Rides are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath       The gzipped input file from which the Earthquake records are read.
     * @param maxEventDelaySecs  The max time in seconds by which events are delayed.
     * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
     */
    public EarthquakeSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        if (maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }
        this.dataFilePath = dataFilePath;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;
        this.watermarkDelayMSecs = Math.max(maxDelayMsecs, 10000);
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<Feature> sourceContext) throws Exception {
        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));

        generateUnorderedStream(sourceContext);

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;
    }

    private void generateUnorderedStream(SourceContext<Feature> sourceContext) throws Exception {
        long servingStartTime = Calendar.getInstance().getTimeInMillis();
        long dataStartTime;

        Random rand = new Random(7452);
        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
            32,
            Comparator.comparing(o -> o.f0));

        // read first earthquake and insert it into emit schedule
        String line;
        Earthquake earthquake = GSON.fromJson(reader, Earthquake.class);
        Feature earthquakeFeature;
        System.out.println(earthquake);

        if (!earthquake.features.isEmpty()) {
            // read first earthquake
            earthquakeFeature = earthquake.features.get(0);
            // extract starting timestamp
            dataStartTime = getEventTime(earthquakeFeature);
            // get delayed time
            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

            emitSchedule.add(new Tuple2<>(delayedEventTime, earthquakeFeature));
            // schedule next watermark
            long watermarkTime = dataStartTime + watermarkDelayMSecs;
            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
            emitSchedule.add(new Tuple2<>(watermarkTime, nextWatermark));
        } else {
            return;
        }

        if (earthquake.features.size() > 1) {
            earthquakeFeature = earthquake.features.get(1);
        }

        // read rides one-by-one and emit a random earthquakeFeature from the buffer each time
        int i = 2;
        while (emitSchedule.size() > 0 || i < earthquake.features.size()) {
            // insert all events into schedule that might be emitted next
            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            long featureEventTime = earthquakeFeature != null ? getEventTime(earthquakeFeature) : -1;
            while (
                earthquakeFeature != null && ( // while there is a earthquakeFeature AND
                    emitSchedule.isEmpty() || // and no earthquakeFeature in schedule OR
                        featureEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enough rides in schedule
            ) {
                // insert event into emit schedule
                long delayedEventTime = featureEventTime + getNormalDelayMsecs(rand);
                System.out.println(new Date(delayedEventTime));
                System.out.println("xx");
                emitSchedule.add(new Tuple2<>(delayedEventTime, earthquakeFeature));

                // read next earthquakeFeature
                try {
                    earthquakeFeature = earthquake.features.get(i);
                    featureEventTime = getEventTime(earthquakeFeature);
                    System.out.println(new Date(featureEventTime));
                } catch (IndexOutOfBoundsException e) {
                    earthquakeFeature = null;
                    featureEventTime = -1;
                }
                i++;
            }

            // emit schedule is updated, emit next element in schedule
            Tuple2<Long, Object> head = emitSchedule.poll();
            long delayedEventTime = head.f0;

            long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
            long now = Calendar.getInstance().getTimeInMillis();
            long waitTime = servingTime - now;

            Thread.sleep((waitTime > 0) ? waitTime : 0);

            if (head.f1 instanceof Feature) {
                Feature emitFeature = (Feature) head.f1;
                // emit earthquakeFeature
                sourceContext.collectWithTimestamp(emitFeature, getEventTime(emitFeature));
            } else if (head.f1 instanceof Watermark) {
                Watermark emitWatermark = (Watermark) head.f1;
                // emit watermark
                sourceContext.emitWatermark(emitWatermark);
                // schedule next watermark
                long watermarkTime = delayedEventTime + watermarkDelayMSecs;
                Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
                emitSchedule.add(new Tuple2<>(watermarkTime, nextWatermark));
            }
        }
    }

    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    public long getEventTime(Feature feature) {
        return feature.properties.time;
    }

    public long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs / 2;
        while (delay < 0 || delay > maxDelayMsecs) {
            delay = (long) (rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.gzipStream != null) {
                this.gzipStream.close();
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.gzipStream = null;
        }
    }

}
