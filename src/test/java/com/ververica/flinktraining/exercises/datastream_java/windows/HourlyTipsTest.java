/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.google.common.collect.Lists;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.ververica.flinktraining.solutions.datastream_java.windows.HourlyTipsSolution;
import org.apache.flink.api.java.tuple.Tuple3;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HourlyTipsTest extends TaxiRideTestBase<Tuple3<Long, Long, Float>> {

	private static Testable javaExercise = () -> HourlyTipsExercise.main(new String[]{});

	@Test
	public void testOneDriverOneTip() throws Exception {
		TaxiFare one = testFare(1, t(0), 1.0F);

		TestFareSource source = new TestFareSource(
				one
		);

		Tuple3<Long, Long, Float> max = new Tuple3<>(t(60), 1L, 1.0F);

		ArrayList<Tuple3<Long, Long, Float>> expected = Lists.newArrayList(max);

		assertEquals(expected, results(source));
	}

	@Test
	public void testTipsAreSummedByHour() throws Exception {
		TaxiFare oneIn1 = testFare(1, t(0), 1.0F);
		TaxiFare fiveIn1 = testFare(1, t(15), 5.0F);
		TaxiFare tenIn2 = testFare(1, t(90), 10.0F);

		TestFareSource source = new TestFareSource(
				oneIn1,
				fiveIn1,
				tenIn2
		);

		Tuple3<Long, Long, Float> hour1 = new Tuple3<>(t(60), 1L, 6.0F);
		Tuple3<Long, Long, Float> hour2 = new Tuple3<>(t(120), 1L, 10.0F);

		ArrayList<Tuple3<Long, Long, Float>> expected = Lists.newArrayList(hour1, hour2);

		assertEquals(expected, results(source));
	}

	@Test
	public void testMaxAcrossDrivers() throws Exception {
		TaxiFare oneFor1In1 = testFare(1, t(0), 1.0F);
		TaxiFare fiveFor1In1 = testFare(1, t(15), 5.0F);
		TaxiFare tenFor1In2 = testFare(1, t(90), 10.0F);
		TaxiFare twentyFor2In2 = testFare(2, t(90), 20.0F);

		TestFareSource source = new TestFareSource(
				oneFor1In1,
				fiveFor1In1,
				tenFor1In2,
				twentyFor2In2
		);

		Tuple3<Long, Long, Float> hour1 = new Tuple3<>(t(60), 1L, 6.0F);
		Tuple3<Long, Long, Float> hour2 = new Tuple3<>(t(120), 2L, 20.0F);

		ArrayList<Tuple3<Long, Long, Float>> expected = Lists.newArrayList(hour1, hour2);

		assertEquals(expected, results(source));
	}

	private long t(int n) {
		return new DateTime(2000, 1, 1, 0, 0, DateTimeZone.UTC).plusMinutes(n).getMillis();
	}

	private TaxiFare testFare(long driverId, long startTime, float tip) {
		return new TaxiFare(0, 0, driverId, new DateTime(startTime), "", tip, 0F, 0F);
	}

	protected List<Tuple3<Long, Long, Float>> results(TestFareSource source) throws Exception {
		Testable javaSolution = () -> HourlyTipsSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), javaExercise, javaSolution);
	}

}
