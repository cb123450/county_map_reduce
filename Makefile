JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
PATH:=${JAVA_HOME}/bin:${PATH}

LIBS=/home/sonny/hadoop-3.3.6/share/hadoop
NEW_CLASSPATH=${LIBS}/mapreduce/*:${LIBS}/common/*:${LIBS}/common/lib/*:${CLASSPATH}

SRC = $(wildcard *.java)

all: build

build: ${SRC}
	${JAVA_HOME}/bin/javac -Xlint -classpath ${NEW_CLASSPATH} ${SRC}
	${JAVA_HOME}/bin/jar cvf build.jar *.class lib

clean:
	rm *~


# ../hadoop-3.3.6/bin/hadoop jar build.jar MapReduce ../us-counties-2020.csv output
