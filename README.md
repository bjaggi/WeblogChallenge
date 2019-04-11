

# WeblogChallenge
## Approach
This applicaton was built using spark & scala. Application has been designed and tested to run both in the batch mode(Ideal) and streaming mode(read new file in the directory). Since the code was developed and run on a laptop. To run of mac simply clone the project and run the PaytmBatchAnalytics file, to run on windows we require the winutils and configure hadoop.home.dir home
```$xslt
System.setProperty("hadoop.home.dir", "C:\\Softwares\\WinUtils");
```
To enable spark  logs( use this for local testing only), uncomment this line
```$xslt
Logger.getLogger("org").setLevel(Level.ERROR)
```


To run the job on the cluster we need access to a cluster and run a spark
## Output 
The output of the code has been saved to git for easy interpretation:

   #### Output from Real Data in Batch Mode (PaytmBatchAnalytics.scala)
    https://github.com/bjaggi/WeblogChallenge/blob/master/Real_Data_Spark_Batch_Mode_Results.txt
    
    
   #### Output from Sample Data in Streaming Mode (PaytmStreamingAnalytics.scala)
    https://github.com/bjaggi/WeblogChallenge/blob/master/Sample_Mock_Data_Spark_Batch_Mode_Results.txt


Spark UI : http://localhost:4040/jobs/

## To Run the Job on the Cluster  :   

``` 
spark-submit 
       --master yarn-client \
       --num-executors 2 \
       --driver-memory 512m \
       --executor-memory 512m \
       --executor-cores 1 \
       --deploy-mode cluster\client \
       --class com.eva.app.PaytmAnalytics \
       weblogchallenge_2.11-1.0.jar 
       
```


# Utils To create test file for Streaming :
``` 
cat  data/sample_click_stream.txt >  data/unit_test1
cat  data/sample_click_stream.txt >  data/unit_test2

``` 

## References
https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.1.0/running-spark-applications/content/running_sample_spark_2_x_applications.html

https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/clickstream/PageViewStream.scala


