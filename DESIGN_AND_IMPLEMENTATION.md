# Guild Data Engineering Project
### Task 1: Schema Design

Given that the requirements were to design a data model that 
serves as a source to a REST API, I designed a transactional normalized
schema.  This gives us more flexibility to meet the needs of our API if 
we want to provide various endpoints to return data in various different structures.

See picture below.  Please do note I have not included all of the columns in each table 
to make the chart more visually appealing.  For a list of all tables, columns and types, 
please see the **Appendix** section at the end of this document.

 ![alt text](./movies_erd.png)

### Task 2: Data ETL Implementation

I unzipped the files in the provided link manually and staged them in the
`s3://guild-test-raw` bucket to simulate what an ETL process in a real-life scenario
might look.  My main ingestion logic is located in the movie_processor_pyspark.py file and 
leverages the `pyspark` API in Python.  *This means the script will have to run on local Spark
or in a Spark cluster*.  For this test, I have spun up a small EMR cluster that hosts Spark which
I will reference later.  I chose pyspark for the following reasons:
* I am most proficient in Python.
* Spark is highly scalable: If the files grew to be 1000x what they are now, 
the EMR or other hosted Spark cluster could easily be scaled up to meet our compute and memory needs.
* Spark is highly tunable: If we needed to create better performance on an existing cluster, we have many knobs
to adjust.
* The Dataframe API in Spark makes transforming various different file types and data structures simple and readable.

Furthermore, I took some opinionated approaches to ingesting the data for simplicity purposes.  If I had more time I probably 
would have made this more configurable.  I have been moving the data to my personal AWS production S3 bucket, `guild-test-production`.
You will be able to specify the exact prefix in the commands provided below.

Although this data will likely go into and AWS RDS instance, I transform the files into JSON with GZIP 
compression to allow for some flexibility in storage and to make debugging and checking easier.  
 
##### Instructions
You will need the **awscli** package in Python installed (I am using **awscli==1.16.122**).  
I have already staged the **movie_processor_pyspark.py** file in S3.

Run the following command to launch the job

```bash
aws emr add-steps --cluster-id <EMR CLUSTER ID> --steps Type=spark,Name=TestJob,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,s3a://guild-test-scripts/spark/movie_process_pyspark.py,s3a://guild-test-production/<S3 PREFIX OF YOUR CHOICE>],ActionOnFailure=CONTINUE
```

I will be sending the `EMR CLUSTER ID` along with the following variables in a personal email:

* aws_access_key_id
* aws_secret_access_key
* region

The whole process takes about five minutes and you can check the results in the S3 path you 
provided within the `guild-test-production` bucket.  Each table has it's own prefix (folder).

### Task 3: REST API Design

I will have a moment of honesty, I have only designed one REST API in my career, but definitely learned 
a lot from it.  However, this is still my weakest area.  I will try to touch on all of your points.

Beginning with the data model, I have found, in my experience, that relational data models provide solid flexibility 
for creating various API endpoints for various reasons.  I would design the API similarly to how the data model is designed. 
For example, I could create more flexible API endpoints that won't take too much work incremental on my end.

My main movies API endpoint to pull movies with main metadata from the movies relations and 
possibly the various movie_<RELATIONSHIP> tables could be:
```bash
/api/v2/movies/{movie_id}/movies.json
```
I could create more flexible endpoints to pull the various metadata associated with each movie such as:

```bash
/api/v2/movies/{movie_id}/genres.json
```

In regards to where I would store the tables I create, I would probably use a small AWS RDS instance with a MySQL or Postgres
engine.  I could benefit from this because I can add further data integrity checks such and primary keys and foreign constraints.
I could also enhance the performance of the API by specifying indexes.  A NoSQL approach such as storing these tables in 
AWS DynamoDB could be an approach as well, but given the number of relations in my data model, it would not be my first choice.

To create the API service, most of my experience has been with Java, specifically the Spring Boot framework (web starter pack).
I like the type safety of Java for developing resilient production services and the Spring Boot starters really make it easy to
develop production ready code with a smaller investment.  I will run the service in a Docker container and use
some sort of of CI/CD software such as **Travis CI** or **CircleCI**.  At the core of the production environment would be
some sort of container orchestration software such as Nomad or Kubernetes based on my experience.

When it came to resiliency and monitoring I would likely have to work with a DevOps professional to help me monitor.  I 
would usually ask them how to configure the service to stay up and potentially alert me if something is wrong or read documentation
they have provided.


