FROM openjdk:11

COPY examples/target/examples-*-jar-with-dependencies.jar .

CMD ["java", "-cp", "examples-*-jar-with-dependencies.jar", "com.automq.elasticstream.client.examples.longrunning.LongRunning"]
