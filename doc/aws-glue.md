# Use this library with AWS Glue

Glue is one of the "flavours" we can use Spark in AWS.

## Requirements

To run this simple example, you'll need an AWS account. You'll create an S3 bucket, an SQS queue and the Glue job.

You'll spend no more than US$ 0.50.

## Configuring access

In this example, your Glue job will get data from a txt file stored in an S3 bucket and write it to a SQS queue.
But before writing any job, we'll need to create a role for the job execution that has all access needed. In our example, the role is called *SparkSQSRole* and has a policy, *SparkSQSPolicy*, with the following permissions.  

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SQSWrite",
      "Effect": "Allow",
      "Action": [
        "sqs:GetQueueUrl",
        "sqs:SendMessage"
      ],
      "Resource": [
        "arn:aws:sqs:us-east-1:831962505547:SparkSQS"
      ]
    },
    {
      "Sid": "AllObjectActions",
      "Effect": "Allow",
      "Action": "s3:*Object",
      "Resource": [
        "arn:aws:s3:::aws-glue-assets-831962505547-us-east-1/*"
      ]
    }
  ]
}
```
## Adding libraries

When we create a Glue job, it creates for us an S3 bucket to store the script. We'll use the same S3 bucket to store the file which content we'll put in the SQS queue and also all libraries needed to run this code.
These libraries can be obtained from maven repository using [https://mvnrepository.com/](https://mvnrepository.com/). Download the jars *aws-java-sdk-sqs-1.12.13.jar* and *spark-aws-messaging-0.4.0.jar*.

![We can use mvnrepository to download the jars needed](/doc/assets/glue-download-jar.png)

Upload these files to the S3 bucket.

![Jars uploaded to the S3 bucket, ready to be used](/doc/assets/glue-jars.png)

## Creating the job

In our example, we'll create a Python script to run our job. We'll do that by accessing the Glue service in AWS, clicking on the *ETL jobs* on the menu, selecting "Python Shell script editor" and clicking in Create. This will open an editor where we can put the code below.

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.text("s3://aws-glue-assets-831962505547-us-east-1/input/Poema-de-Sete-Faces.txt")
df.printSchema()
df.write.format("sqs").mode("append")\
    .option("region", "us-east-1")\
    .option("queueName", "SparkSQS")\
    .option("batchSize", "10") \
    .save()

job.commit()
```

There are some important configurations in the Advanced properties, in the Job details section.
- *Dependent JARs path* must be provided with the path of the jars files we uploaded in the previous step, passing the with the fully path like this: *s3://aws-glue-assets-831962505547-us-east-1/jars/aws-java-sdk-sqs-1.12.13.jar,s3://aws-glue-assets-831962505547-us-east-1/jars/spark-aws-messaging-0.4.0.jar*
- *Job parameters* must have the key *--user-jars-first* with the value *true* provided. This is because the Glue environment already have references to the AWS library in the classpath and sometimes this might conflict with the version the library uses. This parameter ensures that we are using the correct version of the AWS client jar.

You can click on the Run button to start the execution of the job.

![Running the job](/doc/assets/glue-runs.png)

After some time, the content of the file can be retrieved from the SQS queue.

![Messages received](/doc/assets/glue-messages-received.png)