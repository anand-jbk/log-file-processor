# log-file-processor
In this application apache spark sql is used processing the log file.

prerequisite:
  HSQLDB server mmust be running. Properties configure for server is as shown below. These properties can be changed in main/resource/application.properties.
  DB_DRIVER_CLASS=org.hsqldb.jdbcDriver
  DB_CONNECTION_URL=jdbc:hsqldb:hsql://localhost/testdb
  DB_USERNAME=SA
  DB_PASSWORD=

Execution Steps:
  1. mvn clean install
  2. go to target directory
  3. java -jar log-file-processor-0.0.1-SNAPSHOT-jar-with-dependencies.jar <path to log file>
