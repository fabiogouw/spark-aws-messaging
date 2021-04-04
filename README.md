# spark-sqs-sink
An example of a custom sink provider for Apache Spark that sends the contents of a dataframe to AWS SQS. It was heavily inspired by the code of this repo https://github.com/aamargajbhiye/big-data-projects.

It grabs the content of the first column of the dataframe and sends it to an AWS SQS queue. It needs the following parameters:
- **access key** of the IAM user that has permission to write to the queue
- **secret key** of the IAM user that has permission to write to the queue
- **region** of the queue (us-east-2, for instance)
- **name** of the queue
- **batch size** so we can group N messages in one call

```java
    df.write()
            .format("com.fabiogouw.spark.SQSSinkProvider")
            .mode(SaveMode.Append)
            .option("accessKey", "XXXXXXXXXXXXXXXXXXX")
            .option("secretKey", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
            .option("region", "us-east-2")
            .option("queueName", "my-test-queue")
            .option("batchSize", "10")
            .save();
```

It needs the *com.amazonaws:aws-java-sdk* package to run, so you can provide it through the *packages* parameter of spark-submit. 

```
spark-submit \ 
--packages com.amazonaws:aws-java-sdk:1.11.991 \ 
--master local \ 
--class com.mypackage.MyExample \ 
build/libs/my-jar.jar
```

It's easy to get lost while undertanding all the classes are needed so we can create a custom sink for Spark. Here's a class diagram to make it a little easy to find yourself. Start at SQSSinkProvider, it's the class that we configure in Spark code as a *format* method's value.

![alt text](/doc/assets/Class%20Diagram-Page-1.png "Logo Title Text 2")
