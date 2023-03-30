You need to have [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

We build and test this SDK with Java 11 and 17. However, we ensure the generated binaries are compatible with Java 8 or higher (independently of the Java version
used for compilation). 

### Build ###
    ./gradlew build

### Run unit/integration tests ###
    ./gradlew test # runs only unit tests
    ./gradlew integrationTest # runs only integration tests

### Running a particular unit/integration test ###
    ./gradlew test --tests RecordBatchTest

### Running a particular test method within a unit/integration test ###
    ./gradlew test --tests sdk.elastic.storage.models.RecordBatchTest.testEncodeAndDecode

### Running a test with a particular system property of the JVM ###
    ./gradlew test -Dmyprop="myvalue"
