package com.test.logfileprocessor.processor;

import com.test.logfileprocessor.exception.LogFileProcessingException;

public interface ILogFileProcessor {

	/**
	 * To parse the log file and persist resultant events
	 * @throws LogFileProcessingException
	 */
	public void process() throws LogFileProcessingException;
}
