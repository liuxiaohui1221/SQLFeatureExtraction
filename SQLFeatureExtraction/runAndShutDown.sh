#!/bin/sh
mvn -DskipTests clean package
mvn exec:java -Dexec.mainClass="MINCFragmentIntent"
#sudo shutdown
