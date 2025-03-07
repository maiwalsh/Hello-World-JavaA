# Use an official OpenJDK runtime as a parent image
FROM openjdk:latest

# Set the working directory in the container
WORKDIR /app

# Copy the Java file to the container
COPY Hello.java .

# Compile the Java program
RUN javac Hello.java

# Set the command to run the compiled Java program
CMD ["java", "HelloWorld"]
