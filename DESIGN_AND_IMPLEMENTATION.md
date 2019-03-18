# Guild Data Engineering Project
### Task 1: Schema Design

Given that the requirements were to design a data model that 
serves as a source to a REST API, I designed a transactional normalized
schema.  This gives me more flexibility to create different endpoints that 
return data in various different structures.

See picture below.  Please do note I have not included all of the columns in each table 
to make the chart more visually appealing.  For a list of all tables, columns and types, 
please see the **Appendix** section at the end of this document.

 ![alt text](./movies_erd.png)

### Task 2: Data ETL Implementation

I unzipped the files in the provided link manually and staged them in the
`s3://guild-test-raw` bucket to simulate what an ETL process in a real-life scenario
might look.  My main ingestion logic is located in the **movie_processor_pyspark.py** file and 
leverages the `pyspark` API in Python.  *This means the script will have to run on local Spark
or in a Spark cluster*.  For the purposes of this test, I have spun up a small EMR cluster that hosts Spark which
I will reference later.  I chose pyspark for the following reasons:
* I am most proficient in Python.
* Spark is highly scalable: If the files grew to be 1000x what they are now, 
the EMR or other hosted Spark cluster could easily be scaled up to meet our compute and memory needs.
* Spark is highly tunable: If we needed to create better performance on an existing cluster, we have many knobs
to adjust.
* The Dataframe API in Spark makes transforming various different file types and data structures simple and readable.

Furthermore, I took some opinionated approaches to ingesting the data for the purpose of simplicity.  If I had more time I probably 
would have made this more configurable.  I have been moving the data to my personal AWS production S3 bucket, `guild-test-production`.
You will be able to specify the exact prefix in the commands provided below.

Although this data will likely go into an AWS RDS instance, I transform the files into JSON with GZIP 
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
for creating various API endpoints.  I would design the API similarly to how the data model is designed. 
For example, I could create more flexible API endpoints that won't take too much incremental work on my end.

My main movies API endpoint to pull movies with main metadata from the movies relation (and 
possibly the various movie_RELATIONSHIP tables) could be:
```bash
/api/v1/movies/{movie_id}/movies.json
```
I could additionally create more flexible endpoints to pull specific metadata associated with each movie such as:

```bash
/api/v1/movies/{movie_id}/genres.json
```

In regards to where I would store the tables I create, I would probably use a small AWS RDS instance with a MySQL or Postgres
engine.  I could benefit from this because I can add further data integrity checks such and primary keys and foreign constraints.
I could also enhance the performance of the API by specifying additional indexes on certain tables.  A NoSQL approach such as storing these tables in 
AWS DynamoDB could work as well, but given the number of relations in my data model, it would not be my first choice.

To create the API service, most of my experience has been with Java, specifically the Spring Boot framework (web starter pack).
I like the type safety of Java for developing resilient production services and the Spring Boot starters really make it easy to
develop production ready code with a smaller investment.  I will run the service in a Docker container and use
some sort of of CI/CD software such as **Travis CI** or **CircleCI**.  At the core of the production environment would be
some sort of container orchestration software such as Nomad or Kubernetes based on my experience.

When it comes to resiliency, monitoring and authentication, I would likely have to work with a DevOps professional to help me.  I 
would usually ask them how to configure the service to stay up and potentially alert me if something is wrong or read documentation
they have provided.

Since these raw movie files are coming once a month I would probably use some sort of programmatic ETL software or framework to
orchestrate the monthly file ingestion process (movie_processor_pyspark.py).  I prefer Apache Airflow.  After the files are processed
I would use Airflow (or something similar) again to ingest the data into the database.  I would load the data using **Apache Sqoop**
so I could potentially parallelize the load if the data files got large enough.
  
### Appendix

##### Full Data Model

* movies
    * id - int
    * adult - bool
    * budget - float
    * imdb_id - string
    * original_language - string
    * original_title - string
    * popularity - string
    * poster_path - string
    * production_countries - string,
    * release_date - string (could make date),
    * revenue - float,
    * runtime - float,
    * spoken_languages - string,
    * status - string,
    * title - string,
    * video - string,
    * vote_average - float,
    * vote_count - flaot
* actors
    * id - int
    * name - string
    * profile_path - string
* collections
    * id - int
    * name - string
    * poster_path - string
* crews
    * id - int
    * name - string
    * profile_path - string
* genres
    * id - int
    * name - string
* links/links_small
    * id - int
    * imdb_id - string
    * tmb_id - int
* movie_characters
    * movie_id - int
    * cast_id - int
    * cast_order - string
    * character - string
    * gender - string
    * actor_id  - int        
* movie_collections
    * movie_id - int
    * collection_id - int
* movie_crews
    * movie_id - int
    * department - string
    * credit_id - string
    * crew_id - int
* movie_genres
    * movie_id - int
    * genre_id - int    
* movie_keywords
    * movie_id - int
    * keyword_id - int    
* movie_production_companies
    * movie_id - int
    * production_company_id - int  
* production_companies
    * id - int
    * name - string
* ratings/ratings_small
    * user_id - int
    * movie_id - int    
    * rating - float
    
##### Follow-up: If I had more time
I wasn't able to do everything I would want in building a production application.  However, I think
what I was able to produce was a decent v1.  Here is a list of things I would build on in v2.

* I would orchestrate this using a dockerized Apache Airflow deployment.
* I would spend more time cleansing the data.  I did some regex parsing, but noticed some
records looked "dirty" (i.e. movie records with no movie id).  I chose to spend my time developing
the core implementation for this.
* I would write some unit tests and integration tests.
* I would write this in Scala and created a configurable JAR.
