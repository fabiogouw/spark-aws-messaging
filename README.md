# Spark - AWS SQS Sink
A custom sink provider for Apache Spark that sends the contents of a dataframe to AWS SQS.

It grabs the content of the first column of the dataframe and sends it to an AWS SQS queue. It needs the following parameters:
- **region** of the queue (us-east-2, for instance)
- **name** of the queue
- **batch size** so we can group N messages in one call

```java
    df.write()
            .format("sqs")
            .mode(SaveMode.Append)
            .option("region", "us-east-2")
            .option("queueName", "my-test-queue")
            .option("batchSize", "10")
            .save();
```

The dataframe must have a column called 'value' (string), because this column will be used as the body of each message.
Also, the dataframe may have a column called 'msg_attributes' (array of maps of [string, string]). In this case, the library will add each map as a message attribute.

Don't forget you'll need to configure the default credentials in your machine before running the example. See 
[Configuration and credential file settings](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) for more information.

It needs the *com.amazonaws:aws-java-sdk* package to run, so you can provide it through the *packages* parameter of spark-submit. 

```
spark-submit \ 
--packages com.amazonaws:aws-java-sdk:1.12.12 \ 
--jars build/libs/spark-aws-messaging-0.3.0.jar \
--master local \ 
--class com.mypackage.MyExample \ 
build/libs/my-jar.jar
```

It's easy to get lost while understanding all the classes are needed, so we can create a custom sink for Spark. Here's a class diagram to make it a little easy to find yourself. Start at SQSSinkProvider, it's the class that we configure in Spark code as a *format* method's value.

![Class diagram showing all the classes needed to implement a custom sink](/doc/assets/Class%20Diagram-Page-1.png "Class diagram showing all the classes needed to implement a custom sink")
