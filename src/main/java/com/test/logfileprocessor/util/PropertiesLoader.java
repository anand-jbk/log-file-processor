package com.test.logfileprocessor.util;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.test.logfileprocessor.constants.Constants;

/**
 * This class is used for reading and serving application properties.
 * @author anand
 *
 */
public class PropertiesLoader {

	private static Logger LOGGER = LoggerFactory.getLogger(PropertiesLoader.class);
	private static PropertiesLoader instace = new PropertiesLoader();

	private Properties properties = new Properties();

	private PropertiesLoader() {
		try {
			String env = System.getProperty("ENV");
			StringBuilder propertiesFile = new StringBuilder(Constants.APPLICATION_PROPERTIES_NAME);
			if (env != null) {
				propertiesFile.append("_").append(env);
			}
			propertiesFile.append(".").append(Constants.APPLICATION_PROPERTIES_EXTENSION);
			InputStream inputStream = PropertiesLoader.class.getClassLoader()
					.getResourceAsStream(propertiesFile.toString());
			properties.load(inputStream);
			inputStream.close();
		} catch (Exception e) {
			LOGGER.error("Exception occured while loading application properties");
		}
	}

	private Properties getProperties() {
		return properties;
	}

	private static PropertiesLoader getInstace() {
		return instace;
	}

	/**
	 * To get value for passed <code>key</code> from application.properties.
	 * @param key
	 * 			to get values for
	 * @return
	 * 			value for passed <code>key</code> from applciation.properties
	 */
	public static String getProperty(String key) {
		return getInstace().getProperties().getProperty(key);
	}

}