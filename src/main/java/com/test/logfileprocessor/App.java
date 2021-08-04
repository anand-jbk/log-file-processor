package com.test.logfileprocessor;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.test.logfileprocessor.exception.LogFileProcessingException;
import com.test.logfileprocessor.processor.ILogFileProcessor;
import com.test.logfileprocessor.processor.SparkBasedLogFileProcessor;

public class App {

	private static Logger LOGGER = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) {
		LOGGER.debug("Input arguments size {}", args.length);
		if (args.length != 1) {
			LOGGER.error("Please provide (Only) log file path");
			System.exit(1);
		}
		String filePath = args[0];
		LOGGER.debug("Given file path is {}", filePath);
		ILogFileProcessor logFileProcessor = new SparkBasedLogFileProcessor(filePath);
		try {
			StopWatch watch = new StopWatch();
			watch.start();
			logFileProcessor.process();
			watch.stop();
			LOGGER.info("Log file processing is completed successfully in {}s ", watch.getTime(TimeUnit.SECONDS));
		} catch (LogFileProcessingException e) {
			LOGGER.error("Log file processing is failed", e);
		}
		
	}
}
