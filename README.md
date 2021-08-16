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

The dataframe must have a column called **value** (string), because this column will be used as the body of each message.
Also, the dataframe may have a column called **msg_attributes** (array of maps of [string, string]). In this case, the library will add each map as a message attribute.

Don't forget you'll need to configure the default credentials in your machine before running the example. See 
[Configuration and credential file settings](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) for more information.

It also needs the *com.amazonaws:aws-java-sdk-sqs* package to run, so you can provide it through the *packages* parameter of spark-submit.

The following command can be used to run the sample of how to use this library.

``` bash
spark-submit \
--packages com.fabiogouw:spark-aws-messaging:0.3.1,com.amazonaws:aws-java-sdk-sqs:1.12.13 \
test.py sample.txt
```

And this is the test.py file content.

``` python
import sys 
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("File: " + sys.argv[1])

    spark = SparkSession\
        .builder\
        .appName("SQS Write")\
        .getOrCreate()

    df = spark.read.text(sys.argv[1])
    df.show()
    df.printSchema()

    df.write.format("sqs").mode("append")\
        .option("queueName", "test")\
        .option("batchSize", "10") \
        .option("region", "us-east-2") \
        .save()

    spark.stop()
```
This library is available at Maven Central repository, so you can reference it in your project with the following snippet. 

``` xml
<dependency>
    <groupId>com.fabiogouw</groupId>
    <artifactId>spark-aws-messaging</artifactId>
    <version>0.3.1</version>
</dependency>
```

It's easy to get lost while understanding all the classes are needed, so we can create a custom sink for Spark. Here's a class diagram to make it a little easy to find yourself. Start at SQSSinkProvider, it's the class that we configure in Spark code as a *format* method's value.

![Class diagram showing all the classes needed to implement a custom sink](/doc/assets/Class%20Diagram-Page-1.png "Class diagram showing all the classes needed to implement a custom sink")

## Sending messages to a queue in another AWS Account
You might have an architecture where the Spark job and the SQS are in different AWS accounts. In that case, you can specify one extra option to make the writer aware of which account to use.

```java
.option("queueOwnerAWSAccountId", "123456789012")
```