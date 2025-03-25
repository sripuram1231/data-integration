package Data.Integration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DataIntegrationApplication {
	private static final Logger logger = LogManager.getLogger(DataIntegrationApplication.class);

	public static void main(String[] args) {
		System.out.println("Hello");
		logger.info("This is an info message");
		//logger.error("This is an error message", new Exception("Something went wrong"));

	}

}
