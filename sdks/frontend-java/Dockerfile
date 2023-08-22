FROM openjdk:11

COPY tools/target/tools-*-jar-with-dependencies.jar ./tools.jar

CMD ["java", "-cp", "tools.jar", "com.automq.elasticstream.client.tools.longrunning.LongRunning"]
