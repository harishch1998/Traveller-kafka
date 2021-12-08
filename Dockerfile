FROM openjdk:11
ADD consumer/target/consumer-0.0.1-SNAPSHOT.jar consumer.jar
ENTRYPOINT ["java", "-jar","consumer.jar"]
EXPOSE 8080



