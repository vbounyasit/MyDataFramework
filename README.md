# DataEng Framework
_A Scala ETL Framework based on [Apache Spark](https://spark.apache.org/) for Data engineers._

The project has been released on Maven central ! See [Wiki pages](https://github.com/vbounyasit/DataFlow/wiki) to start right away.

## Getting started

You can import this library by adding the following to the dependencies in your `pom.xml` file :
```xml
<dependency>
    <groupId>io.github.vbounyasit</groupId>
    <artifactId>dataflow</artifactId>
    <version>1.1.0-SNAPSHOT</version>
</dependency>
```

## Overview
This is a project I have been working on for a few months, 
with the purpose of allowing **Data engineers** to write efficient, 
clean and bug-free data processing projects with [Apache Spark](https://spark.apache.org/).

The main objective of this Framework is to make the engineer mainly focus on writing the 
**Transformation** logic of large scale ETL projects, rather than writing the entire application layout over and over, 
by providing only the necessary information for input data sources extraction, output data persistence, and writing 
the data transformation logic.

## About me
I am a Data Engineer who have been working with [Apache Spark](https://spark.apache.org/) for almost 4 years and have found a particular interest in this field.

The reason I have decided to write this project was primarily for learning purposes, but more importantly, because through
my experience at companies with some large scale data processing projects, I have realized that some parts of my projects were
almost or exactly the same from one project to another (such as data extraction, result data persistence or Unit/Integration tests).

I felt that something could be done about this, and that the data engineer community could have a use for something like that. Moreover,
in order to deepen my understanding of Spark and the Scala language, what better way to practice than by building my own
project from scratch?

## Context & Requirements
_This section will cover the requirements as well as the main use case for this project to help you determine
whether this Framework is for you._

As a Data engineer, you are expected to oversee or take part in the data processing ecosystem at your company. 
More specifically, you are expected to write data processing applications following certain rules provided by the business 
or other working teams such as the data scientists.

You must have realized that no matter how many ETL projects you create, the vast majority of them follow 
a certain common structure that you have to rewrite every time. The only thing that really needs your full attention
is the transformation logic. Indeed, when you have figured out where you get your data from, and what to do with
the result of your pipelines, the logic does not change much from one project to another.
What's important here is **the actual data pipeline**. You want to write the most optimized and efficient logic.

I have written this Framework **for that very purpose**. Aside from some configuration files creation, you will only have to focus on setting up your
transformation pipelines, and configure your Unit/Integration tests. Those alone should allow you to have 
a perfectly working and boilerplate-free project with good test coverage.

These are the requirements for this Framework : 

- The project is in Scala. Therefore, you will need some proficiency with this language.
- You need to have a functional Spark cluster with a cluster management system, as any project based on this will be packaged 
and **submitted as a Spark application** (with the spark-submit command).

    **Note**: _This only applies in case you are planning on bringing your application into **production**. You can perfectly make use of this Framework even if you only have your computer with you. 
You will be able to write your pipelines and test them with the different features offered by this Framework._
- All of the input data for your Spark jobs will have to be queryable from [Spark Hive](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html) (sources are queried with `spark.read.table(s"$database.$table")`)
- You will have to implement your own logic for handling the output result from your Spark jobs(storing them into HDFS, sending them to the business, etc).
After running your Spark job, you will obtain a resulting `DataFrame` object.

The two first requirements are quite obvious. However, the two last ones are not. 

&nbsp;

##### Spark Hive data querying
I assumed that the input data sources should be queryable through a single endpoint because I think this is the best
way to do it. Indeed, it is true that data itself can come in every possible format, be it json, csv, or even text files with weird patterns. 
However, It would be a mess to have to handle data extraction and structuring in an ETL project, 
especially if they can come in tons of possible formats.
Therefore, I have set that particular requirement with [Spark Hive](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html) querying, which I think is a good solution.

##### Result data persistence
Since the method to persist the resulting data from Spark jobs differs greatly from one ecosystem to another, 
I decided to leave that part for the engineers. 
Especially when the way to deliver the resulting data is most likely to be determined by whoever needs them.

For information, at my previous company, we used to store the data on HDFS 
as parquet files, queryable through [Spark Hive](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html),
and send a copy of it to the business in csv files for their own use.

_Note : The requirements above might change, depending on people feedback and suggestions_

&nbsp;

## Getting Started & Documentation
The DataFlow Framework maintains reference documentation on
Github [wiki pages](https://github.com/vbounyasit/DataFlow/wiki), and will have a 
better support later on as the wiki construction progresses.

If you think this Framework is the solution you have been looking for, you can head over to
the [wiki](https://github.com/vbounyasit/DataFlow/wiki) and start making your own DataFlow project !

## Libraries used
- [Apache Spark](https://spark.apache.org/)
- [Cats](https://github.com/typelevel/cats)
- [PureConfig](https://github.com/pureconfig/pureconfig)
- [Scopt](https://github.com/scopt/scopt)
- [Slf4j](https://www.slf4j.org/)

## License
The DataFlow Framework is released under version 2.0 of the [Apache License](http://www.apache.org/licenses/LICENSE-2.0).
