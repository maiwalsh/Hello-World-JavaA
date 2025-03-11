# Use an official OpenJDK runtime as a parent image
FROM maven:3.9.6-eclipse-temurin-21

# Set the working directory in the container
WORKDIR /app

# Copy the Java file to the container
# COPY Hello.java .

# Add dependencies for Maven and other tools if required
COPY ./target/HelloWorldKafka-1.0-SNAPSHOT.jar /app/HelloWorldKafka.jar

# Compile the Java program
# RUN javac Hello.java

# Set the entry point to run the Java application
CMD ["java", "-jar", "/app/HelloWorldKafka.jar"]
