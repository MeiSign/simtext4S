Work in Progress

Spark Job implementation of Dick Grune's SIM (http://dickgrune.com/Programs/similarity_tester/)

# Packaging simtext4s as jar for spark on Windows (installation of sbt is required):

    git clone https://github.com/MeiSign/simtext4S.git
    // edit config file in resources
    cd simtext4s
    sbt package

# Packaging simtext4s as jar for spark on Unix (installation of sbt is not required):

    git clone https://github.com/MeiSign/simtext4S.git
    cd simtext4s
    mv resources/application.conf.template resources/application.conf
    vim resources/application.conf
    ./sbt package
