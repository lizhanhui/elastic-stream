FROM openjdk:11

COPY examples/target/examples-1.0-SNAPSHOT-jar-with-dependencies.jar .

CMD ["java", "-cp", "examples-1.0-SNAPSHOT-jar-with-dependencies.jar", "com.automq.elasticstream.client.examples.longrunning.LongRunning"]
