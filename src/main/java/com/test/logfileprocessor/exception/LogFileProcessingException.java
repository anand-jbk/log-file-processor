package com.test.logfileprocessor.exception;

public class LogFileProcessingException extends Exception {

	private String errorMessage;
	private String errorCode;

	public LogFileProcessingException(String errorMessage, String errorCode) {
		super();
		this.errorMessage = errorMessage;
		this.errorCode = errorCode;
	}

	public LogFileProcessingException(String errorMessage, String errorCode, Exception e) {
		super(e);
		this.errorMessage = errorMessage;
		this.errorCode = errorCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public String getErrorCode() {
		return errorCode;
	}

}
