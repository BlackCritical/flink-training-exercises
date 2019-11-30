#Big Data Project

This repository is a fork of the flink-training-exercise (https://github.com/ververica/flink-training-exercises).
A few dependencies were added.  
One of them is org.projectlombok.  
**IF YOU USE AN IDE LIKE INTELLIJ YOU WILL PROBABLY NEED THE LOMBOK PLUGIN.**
**OTHERWISE YOU WILL SEE A LOT OF ERRORS IN THE CODE.**
**BUT THOSE ERRORS SHOULD NOT PREVENT YOU FROM COMPILING THE PROJECT!**

##1. and 2. Data Collection:
I chosen earthquakes as my dataset.
I queried these earthquake datasets from an open API called: https://earthquake.usgs.gov/fdsnws/event/1/
For a documentation about the structure of the dataset you could look at this URL: https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
For a documentation about what each individual property stands for you could read through this URL: https://earthquake.usgs.gov/data/comcat/data-eventterms.php

Inside the ./bigdatareader project is a main class called com.example.bigdatareader.BigDataReaderApplication, which starts a REST-Spring-Boot-Application.
This Project is a Gradle Project which can be build by executing "gradle build" or "gradlew build" inside a shell opened in ./bigdatareader.

This REST-Server contains only one Endpoint: http://localhost:8099/start .
By Calling the Endpoint a async call to https://earthquake.usgs.gov/fdsnws/event/1/query 
with the QueryParameters:

- format=geojson, 
- starttime=XXXX-XX-XX,
- endtime=XXXX-XX-XX

will be made, for every day from 2014-01-01 to 2019-12-31.  
The first call, with a starttime higher than the current date, will cause an exception, 
but that is okay, because the responses of all calls prior to this, will be written to a file called earthquake.json.
The DataSet from 2014-01-01 to 2019-11-05 contained 813445 Features (com.ververica.flinktraining.project.model.Feature).
This DataSet will be used for offline and online analysis.  
Inside com/ververica/flinktraining/exercises/datastream_java/utils/ExerciseBase is a constant called pathToALLEarthquakeData which contains the path to the archive.  
Inside com.ververica.flinktraining.project.TransformEarthquakeJSON are a few helper functions i used through out the project to look into or modify the DataSet.
See the doc comments on those functions for more information!

##3. The stream (online) metric computation:
The stream metric computation can be started by executing the com.ververica.flinktraining.project.EarthquakeStreamProjectExercise main class.
Which metrics are computed can be read under "Chosen Metrics"!

##4. The stream (offline) metric computation:
The stream metric computation can be started by executing the com.ververica.flinktraining.project.EarthquakeBatchProjectExercise main class.
Which metrics are computed can be read under "Chosen Metrics"!

###Chosen Metrics:

1. The earthquakes magnitude (located inside the "mag" property)
2. The method or algorithm used to calculate the preferred magnitude for the event. (located inside the "magType" property)
3. Status is either automatic or reviewed. Automatic events are directly posted by automatic processing systems and have not been verified or altered by a human. Reviewed events have been looked at by a human. (located inside the "status" property. Typical Values “automatic”,“reviewed”,“deleted”)
4. The SIG number describing how significant the event is. Larger numbers indicate a more significant event. (This value is determined on a number of factors, including: magnitude, maximum MMI, felt reports, and estimated impact. and located inside the "sig" property)
5. "Tsunami" This flag is set to "1" for large events in oceanic regions and "0" otherwise. The existence or value of this flag does not indicate if a tsunami actually did or will exist! (located inside the "tsunami" property)


##5. Compare the online statistics with the offline computed statistics
The visualizations are comparing online with offline statistics. 
But since both analyze the same dataset, they are almost exactly the same.
The only minor differences are due to the fact that the streaming window size is 100, but the dataset size is 813445.
That means, that 45 (813445 % 100 = 45) features are not processed, because they are drop after the stream completes/terminates.

##6. Study options for making predictions about 3 statistics
I got no experience in the field of Machine Learning.
Because of that i got not enough experience to use a ML library.
I decided to implement 3 simple machine learning algorithm on my own:

- com.ververica.flinktraining.project.magnitude.stream.MLMagnitudeHistogram.machineLearningPrediction1
- com.ververica.flinktraining.project.magnitude.stream.MLMagnitudeHistogram.machineLearningPrediction2
- com.ververica.flinktraining.project.location.MLMapEventsToLocation.machineLearningPrediction3

1. Prediction: Calculates incrementally the average for all magnitudes processed so far.
The average value will be used as prediction.  
2. Prediction: Calculate the likelihood for the next earthquake feature to be reviewed.
3. Prediction: Remembers how often every country name occurred and predicts next country, by guessing the country with the most appearances as next country.

Since all prediction algorithm are inside a RichFlatMapFunction on a keyed stream, predictions will be made for every key group individually.
All those stream are keyed on the "tsunami" property.
That means, there are only two key groups one with a tsunami's and one without tsunami's!

All predictions are printed to the console!

##7. Create a visualization and prepare the demo
Inside the visualization folder are 3 excel sheets as visualization.

##8. Prepare the demo submission
The video is inside the "video" folder!
