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

package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.google.common.collect.Lists;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RideCleansingTest extends TaxiRideTestBase<TaxiRide> {

	private static Testable javaExercise = () -> RideCleansingExercise.main(new String[]{});

	@Test
	public void testInNYC() throws Exception {
		TaxiRide atPennStation = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);

		TestRideSource source = new TestRideSource(atPennStation);

		assertEquals(Lists.newArrayList(atPennStation), results(source));
	}

	@Test
	public void testNotInNYC() throws Exception {
		TaxiRide toThePole = testRide(-73.9947F, 40.750626F, 0, 90);
		TaxiRide fromThePole = testRide(0, 90, -73.9947F, 40.750626F);
		TaxiRide atNorthPole = testRide(0, 90, 0, 90);

		TestRideSource source = new TestRideSource(toThePole, fromThePole, atNorthPole);

		assertEquals(Lists.newArrayList(), results(source));
	}

	private TaxiRide testRide(float startLon, float startLat, float endLon, float endLat) {
		return new TaxiRide(1L, true, new DateTime(0), new DateTime(0),
				startLon, startLat, endLon, endLat, (short)1, 0, 0);
	}

	protected List<?> results(TestRideSource source) throws Exception {
		Testable javaSolution = () -> com.ververica.flinktraining.solutions.datastream_java.basics.RideCleansingSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), javaExercise, javaSolution);
	}

}
