package com.test.logfileprocessor.processor;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.test.logfileprocessor.constants.Constants;
import com.test.logfileprocessor.constants.DBConstants;
import com.test.logfileprocessor.exception.LogFileProcessingException;
import com.test.logfileprocessor.util.PropertiesLoader;

/**
 * This class process log file using spark.
 * @author anand
 *
 */
public class SparkBasedLogFileProcessor implements ILogFileProcessor {

	private static Logger LOGGER = LoggerFactory.getLogger(SparkBasedLogFileProcessor.class);
	
	private String filePath;
	
	public SparkBasedLogFileProcessor(String filePath) {
		this.filePath = filePath;
	}
	
	public void process() throws LogFileProcessingException{
		LOGGER.info("Entry parse");
		try {
			SparkSession sparkSession = SparkSession.builder().appName(Constants.SPARK_APPLICATION_NAME).master("local[*]").getOrCreate();
			
			 StructType schema = new StructType()
				      .add("id", DataTypes.StringType, false)
				      .add("host", DataTypes.StringType, true)
				      .add("timestamp", DataTypes.LongType, false)
				      .add("state", DataTypes.StringType, false)
				      .add("type", DataTypes.StringType, true);
			
			Dataset<Row> logEntriesDF = sparkSession.read().schema(schema).json(filePath);
			Dataset<Row> eventsDF = getEvents(logEntriesDF,sparkSession);
			writeToEventsTable(eventsDF);
			sparkSession.stop();
		} catch (Exception e) {
			LOGGER.error("Exception occoured while processing log file", e);
			throw new LogFileProcessingException("Exception occoured while processing log file", "",e);
		}
	}

	/**
	 * To get events from log entries.
	 * @param logEntriesDF
	 * 			 holds all log entries
	 * @param sparkSession
	 * @return
	 * 		Events out of passed log entries
	 */
	public Dataset<Row> getEvents(Dataset<Row> logEntriesDF, SparkSession sparkSession) {
		LOGGER.info("Entry getEvents");
		
		Dataset<Row> startedEvents = logEntriesDF.filter("state == 'STARTED'");
		startedEvents.createOrReplaceTempView("started_events");

		Dataset<Row> finishedEvents = logEntriesDF.filter("state == 'FINISHED'");
		finishedEvents.createOrReplaceTempView("finished_events");

		Dataset<Row> result = sparkSession.sql(
				"select STRING(se.id), STRING(se.host), STRING(se.type), INT(fe.timestamp-se.timestamp) as duration, INT(fe.timestamp-se.timestamp) > 4 as alert from finished_events fe join started_events se on fe.id == se.id");
		return result;
	}
	
	/**
	 * To persist events
	 * @param eventsDF
	 * 			events to persist
	 */
	public void writeToEventsTable(Dataset<Row> eventsDF) {
		LOGGER.info("Entry writeToEventsTable");
		Properties prop = new Properties();
		prop.setProperty("driver", PropertiesLoader.getProperty(DBConstants.DB_DRIVER_CLASS_KEY));
		prop.setProperty("user", PropertiesLoader.getProperty(DBConstants.DB_USERNAME_KEY));
		prop.setProperty("password", PropertiesLoader.getProperty(DBConstants.DB_PASSWORD_KEY));

		String url = PropertiesLoader.getProperty(DBConstants.DB_CONNECTION_URL_KEY);
		String table = "events";
		eventsDF.write().mode("append")
				.option("createTableColumnTypes", "id varchar(100), host varchar(100), type varchar(100), duration int, alert boolean")
				.jdbc(url, table, prop);
	}
	
	
}
