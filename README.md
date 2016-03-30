# Simtext4s

This Spark Job is a tool to compare large numbers of texts against each other.
The Spark Job implementation is inspired by the program SIM of Dick Grune(http://dickgrune.com/Programs/similarity_tester/)

# Usage
All you have to do is clone the repository,
change the config to your needs and run it on your cluster.
The Job is written with Scala, Windows Users need to install sbt
to package the Jar File.

Unix users can use the provided sbt script for packaging.

## Packaging simtext4s as jar for spark on Unix:

    git clone https://github.com/MeiSign/simtext4S.git
    cd simtext4s
    mv resources/application.conf.template resources/application.conf
    vim resources/application.conf
    ./sbt package


## Packaging simtext4s as jar for spark on Windows:

    git clone https://github.com/MeiSign/simtext4S.git
    // edit config file in resources
    cd simtext4s
    sbt package

## Run it with Spark
Once you have packaged the jar you can run it with the spark-submit script.
The settings are depending of the size of data you want to compare.

Example

    spark-submit
      --class de.simtext.SimText
      --executor-memory 2G
      --driver-memory 2G
      simtext4s-with-spark_2.10-1.0.jar
