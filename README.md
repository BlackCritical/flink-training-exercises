#Big Data Project

This repository is a fork of the flink-training-exercise (https://github.com/ververica/flink-training-exercises).
A few dependencies were added.  
One of them is org.projectlombok.  
**IF YOU USE AN IDE LIKE INTELLIJ YOU WILL PROBABLY NEED THE LOMBOK PLUGIN.**
**OTHERWISE YOU WILL SEE A LOT OF ERRORS IN THE com.ververica.flinktraining.project.model PACKAGE.**
**BUT THOSE ERRORS SHOULD NOT PREVENT YOU FROM COMPILING THE PROJECT!**

##1. and 2. Data Collection:
Inside com.ververica.flinktraining.data_collection is a main class called BigDataReaderApplication, which starts a REST-SpringBoot-Application.
This REST-Server contains only one Endpoint: http://localhost:8080/start .
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
Not done yet

##6. Study options for making predictions about 3 statistics
Not done yet

##7. Create a visualization and prepare the demo
Inside the visualization folder are a few excel sheets as visualization.
