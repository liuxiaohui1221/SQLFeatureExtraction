## About this repo:

This is a best-effort SQL parser which is tailored to the CourseWebsite (MINC) dataset and BusTracker dataset.

MINC dataset is proprietary and cannot be released. The BusTracker dataset is from an earlier work cited in our paper.

This github repo has been initially cloned from https://github.com/UBOdin/EttuBench. On the top of their JSQLParser implementation, we have built our SQL fragment embedding vector creator

Our code for SQL fragment extraction includes:
* SQLFeatureExtraction/src/main/java/MINCFragmentIntent.java
* SQLFeatureExtraction/src/main/java/IntentCreator.java
* SQLFeatureExtraction/src/main/java/SQLParser.java
* SQLFeatureExtraction/src/main/java/SchemaParser.java

The remaining code from https://github.com/UBOdin/EttuBench serves as utility code for our SQL Fragment extraction.
