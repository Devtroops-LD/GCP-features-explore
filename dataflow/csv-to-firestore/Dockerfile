# Flex Template launcher image. Build the jar first: mvn -B package
FROM gcr.io/dataflow-templates-base/java17-template-launcher-base:latest

ARG WORKDIR=/template
WORKDIR ${WORKDIR}

COPY target/csv-to-firestore-bundled.jar ${WORKDIR}/pipeline.jar

ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.devtroops.dataflow.CsvToFirestore"
ENV FLEX_TEMPLATE_JAVA_CLASSPATH="${WORKDIR}/pipeline.jar"

ENTRYPOINT ["/opt/google/dataflow/java_template_launcher"]
