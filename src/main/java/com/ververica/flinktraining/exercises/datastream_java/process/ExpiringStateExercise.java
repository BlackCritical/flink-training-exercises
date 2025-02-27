/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.process;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.PriorityQueue;

/**
 * The "Expiring State" exercise from the Flink training
 * (http://training.ververica.com).
 * <p>
 * The goal for this exercise is to enrich TaxiRides with fare information.
 * <p>
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 */
public class ExpiringStateExercise extends ExerciseBase {
    static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {
    };
    static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {
    };

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
        final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

        final int maxEventDelay = 60;           // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600;    // 10 minutes worth of events are served every second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        DataStream<TaxiRide> rides = env
            .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
            .filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
            .keyBy(ride -> ride.rideId);

        DataStream<TaxiFare> fares = env
            .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
            .keyBy(fare -> fare.rideId);

        SingleOutputStreamOperator processed = rides
            .connect(fares)
            .process(new EnrichmentFunction());

        printOrTest(processed.getSideOutput(unmatchedFares));
//        printOrTest(processed.getSideOutput(unmatchedRides));

        env.execute("ExpiringStateExercise (java)");
    }

    public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

        /* we'll use a PriorityQueue to buffer not-yet-fully-sorted events */
        private ValueState<PriorityQueue<TaxiRide>> rideQueueState = null;
        private ValueState<PriorityQueue<TaxiFare>> fareQueueState = null;

        private ValueState<TaxiRide> pendingTaxiRide;
        private ValueState<TaxiFare> pendingTaxiFare;

        @Override
        public void open(Configuration config) throws Exception {
            ValueStateDescriptor<PriorityQueue<TaxiRide>> descriptorRide = new ValueStateDescriptor<>(
                "sorted-rides", TypeInformation.of(new TypeHint<PriorityQueue<TaxiRide>>() {
            }));
            ValueStateDescriptor<PriorityQueue<TaxiFare>> descriptorFare = new ValueStateDescriptor<>(
                "sorted-fares", TypeInformation.of(new TypeHint<PriorityQueue<TaxiFare>>() {
            }));

            rideQueueState = getRuntimeContext().getState(descriptorRide);
            fareQueueState = getRuntimeContext().getState(descriptorFare);
            pendingTaxiRide = getRuntimeContext().getState(new ValueStateDescriptor<>("pendingTaxiRide", TaxiRide.class));
            pendingTaxiFare = getRuntimeContext().getState(new ValueStateDescriptor<>("pendingTaxiFare", TaxiFare.class));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            PriorityQueue<TaxiRide> rideQueue = rideQueueState.value();
            if (rideQueue != null) {
                long watermark = ctx.timerService().currentWatermark();
                TaxiRide head = rideQueue.peek();
                while (head != null && head.getEventTime() <= watermark) {
                    ctx.output(unmatchedRides, head);
                    rideQueue.remove(head);
                    head = rideQueue.peek();
                }
            }

            PriorityQueue<TaxiFare> fareQueue = fareQueueState.value();
            if (fareQueue != null) {
                long watermark = ctx.timerService().currentWatermark();
                TaxiFare fHead = fareQueue.peek();
                while (fHead != null && fHead.getEventTime() <= watermark) {
                    ctx.output(unmatchedFares, fHead);
                    fareQueue.remove(fHead);
                    fHead = fareQueue.peek();
                }
            }
        }

        @Override
        public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TimerService timerService = context.timerService();
            if (context.timestamp() > timerService.currentWatermark()) {
                PriorityQueue<TaxiRide> queue = rideQueueState.value();
                if (queue == null) {
                    queue = new PriorityQueue<>(100);
                }
                queue.add(ride);
                rideQueueState.update(queue);
                timerService.registerEventTimeTimer(ride.getEventTime());
            }
            if (pendingTaxiFare.value() == null) {
                pendingTaxiRide.update(ride);
            } else {
                out.collect(new Tuple2<>(ride, pendingTaxiFare.value()));
                context.timerService().deleteEventTimeTimer(ride.getEventTime());
                pendingTaxiFare.clear();
            }
        }

        @Override
        public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TimerService timerService = context.timerService();

            if (context.timestamp() > timerService.currentWatermark()) {
                PriorityQueue<TaxiFare> queue = fareQueueState.value();
                if (queue == null) {
                    queue = new PriorityQueue<>(100);
                }
                queue.add(fare);
                fareQueueState.update(queue);
                timerService.registerEventTimeTimer(fare.getEventTime());
            }
            if (pendingTaxiRide.value() == null) {
                pendingTaxiFare.update(fare);
            } else {
                out.collect(new Tuple2<>(pendingTaxiRide.value(), fare));
                context.timerService().deleteEventTimeTimer(fare.getEventTime());
                pendingTaxiRide.clear();
            }
        }
    }
}
