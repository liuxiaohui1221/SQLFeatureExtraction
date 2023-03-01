## About this repo:

This is a best-effort SQL feature vector extractor which is tailored to the CourseWebsite (MINC) dataset and BusTracker dataset. This is a part of our work towards our paper https://dl.acm.org/doi/10.1145/3442338

CourseWebsite (MINC) dataset is proprietary and cannot be released. The BusTracker dataset is from an earlier work http://www.cs.cmu.edu/~malin199/data/tiramisu-sample/ 

The version of the BusTracker dataset we used is available at
* https://www.dropbox.com/s/umqj1dnc7bhvpcw/BusTracker.zip?dl=0

Pre-created SQL fragment vectors for the BusTracker dataset are available at BusTracker/InputOutput/MincBitFragmentIntentSessions

This github repo has been initially cloned from https://github.com/UBOdin/EttuBench. On the top of their JSQLParser implementation, we have built our SQL fragment embedding vector creator

Our code for SQL fragment extraction includes:
* SQLFeatureExtraction/src/main/java/MINCFragmentIntent.java
* SQLFeatureExtraction/src/main/java/IntentCreator.java
* SQLFeatureExtraction/src/main/java/SQLParser.java
* SQLFeatureExtraction/src/main/java/SchemaParser.java

The remaining code from https://github.com/UBOdin/EttuBench serves as utility code for our SQL Fragment extraction.
