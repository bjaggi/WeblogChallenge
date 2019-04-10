

# WeblogChallenge
##Approach
This applicaton was built using spark & scala. Application has been designed and tested to run both in the batch mode and streaming mode. Since the code was ran on a laptop, portions of the code are now commented in effort to finish the job in less than 15 secs. Also portion of the code is commented for rapid development/easy interpretation of logs.

To run the job on the cluster we need access to a cluster and run a spark
## Output 
The output of the code has been saved to git for easy interpretation:

   #### Output from Real Data in Batch Mode (PaytmBatchAnalytics.scala)
    https://github.com/bjaggi/WeblogChallenge/blob/master/Real_Data_Spark_Batch_Mode_Results.txt
    
    
   #### Output from Sample Data in Streaming Mode ()
    https://github.com/bjaggi/WeblogChallenge/blob/master/Sample_Mock_Data_Spark_Batch_Mode_Results.txt


Spark UI : http://localhost:4040/jobs/

##To Run the Job on the Cluster  :   

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


#Utils To create test file for Streaming :
``` 
cat  data/sample_click_stream.txt >  data/unit_test1
cat  data/sample_click_stream.txt >  data/unit_test2

``` 

## Reference
https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.1.0/running-spark-applications/content/running_sample_spark_2_x_applications.html
