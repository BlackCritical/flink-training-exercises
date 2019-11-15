This repository contains examples as well as reference solutions and utility classes for the Apache Flink Training exercises 
found on [http://training.ververica.com](http://training.ververica.com).

Chosen Metrics:

1. The earthquakes magnitude (located inside the "mag" property)
2. The method or algorithm used to calculate the preferred magnitude for the event. (located inside the "magType" property)
3. Status is either automatic or reviewed. Automatic events are directly posted by automatic processing systems and have not been verified or altered by a human. Reviewed events have been looked at by a human. (located inside the "status" property. Typical Values “automatic”,“reviewed”,“deleted”)
4. The SIG number describing how significant the event is. Larger numbers indicate a more significant event. (This value is determined on a number of factors, including: magnitude, maximum MMI, felt reports, and estimated impact. and located inside the "sig" property)
5. "Tsunami" This flag is set to "1" for large events in oceanic regions and "0" otherwise. The existence or value of this flag does not indicate if a tsunami actually did or will exist! (located inside the "tsunami" property)
