FROM maven:3-jdk-8
MAINTAINER David Esner <esnerda@gmail.com>

ENV APP_VERSION 1.1.0
 WORKDIR /home
RUN git clone https://mhumpolec@bitbucket.org/mhumpolec/keboola.git ./  
RUN mvn compile

ENTRYPOINT mvn -q exec:java -Dexec.args=/data  