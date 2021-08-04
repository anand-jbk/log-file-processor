package com.test.logfileprocessor.processor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.test.logfileprocessor.constants.Constants;
import com.test.logfileprocessor.processor.SparkBasedLogFileProcessor;

public class SparkBasedLogFileProcessorTest {

	private SparkBasedLogFileProcessor objectUnderTest = new SparkBasedLogFileProcessor("");

	@Test
	public void whenGetEventsIsCalledAndNoLogEntriesThenNoEventsAreReturned() {
		SparkSession sparkSession = SparkSession.builder().appName(Constants.SPARK_APPLICATION_NAME).master("local[*]")
				.getOrCreate();
		Object[][] logEntries = new Object[][] {};
		Dataset<Row> logEntriesDF = getDataframeForLogEntries(sparkSession, logEntries);

		Dataset<Row> events = objectUnderTest.getEvents(logEntriesDF, sparkSession);

		assertEquals(true, events.isEmpty());
	}

	@Test
	public void whenGetEventsIsCalledAndWithLogEntriesThenCorrectEventsAreReturned() {
		SparkSession sparkSession = SparkSession.builder().appName(Constants.SPARK_APPLICATION_NAME).master("local[*]")
				.getOrCreate();
		Object[][] logEntries = new Object[][] { 
				{ "id_1", "STARTED", 12345l, "APPLICATION_LOG", "host" },
				{ "id_1", "FINISHED", 12346l, "APPLICATION_LOG", "host" },
				{ "id_2", "STARTED", 1l, "APPLICATION_LOG", "host" },
				{ "id_2", "FINISHED", 6l, "APPLICATION_LOG", "host" }
				};
		Object[][] expectedEvents = new Object[][] { 
			{ "id_1", "host", "APPLICATION_LOG", 1l, false },
			{ "id_2", "host", "APPLICATION_LOG", 5l, true }, 
			};
				
		Dataset<Row> logEntriesDF = getDataframeForLogEntries(sparkSession, logEntries);
		Dataset<Row> expectedEventsDF = getDataframeForEvents(sparkSession, expectedEvents);
		
		Dataset<Row> events = objectUnderTest.getEvents(logEntriesDF, sparkSession);
		assertEquals(true, events.exceptAll(expectedEventsDF).isEmpty());
	}

	private Dataset<Row> getDataframeForLogEntries(SparkSession sparkSession, Object[][] data) {
		final StructField id = DataTypes.createStructField("id", DataTypes.StringType, false);
		final StructField type = DataTypes.createStructField("type", DataTypes.StringType, true);
		final StructField host = DataTypes.createStructField("host", DataTypes.StringType, true);
		final StructField state = DataTypes.createStructField("state", DataTypes.StringType, false);
		final StructField timestamp = DataTypes.createStructField("timestamp", DataTypes.LongType, false);

		StructType logEntrySchema = DataTypes.createStructType(new StructField[] { id, state, timestamp, type, host });

		List<Row> rows = new ArrayList<Row>();

		for (int i = 0; i < data.length; i++) {
			rows.add(new GenericRowWithSchema(data[i], logEntrySchema));
		}
		return sparkSession.createDataFrame(rows, logEntrySchema);
	}

	private Dataset<Row> getDataframeForEvents(SparkSession sparkSession, Object[][] data) {
		final StructField id = DataTypes.createStructField("id", DataTypes.StringType, false);
		final StructField type = DataTypes.createStructField("type", DataTypes.StringType, true);
		final StructField host = DataTypes.createStructField("host", DataTypes.StringType, true);
		final StructField duration = DataTypes.createStructField("duration", DataTypes.LongType, false);
		final StructField alert = DataTypes.createStructField("alert", DataTypes.BooleanType, false);
		StructType eventsSchema = DataTypes.createStructType(new StructField[] { id, host, type, duration, alert });

		List<Row> rows = new ArrayList<Row>();

		for (int i = 0; i < data.length; i++) {
			rows.add(new GenericRowWithSchema(data[i], eventsSchema));
		}
		return sparkSession.createDataFrame(rows, eventsSchema);
	}
}
