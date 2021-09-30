FROM openjdk:15-alpine
RUN apk --no-cache add curl
COPY target/KafkaStreamsExample-1.0-SNAPSHOT-jar-with-dependencies.jar demo.jar
EXPOSE 8080
#ENV BOOTSTRAP_SERVERS_CONFIG=kafka:9092
CMD java -Xdebug -jar demo.jar



