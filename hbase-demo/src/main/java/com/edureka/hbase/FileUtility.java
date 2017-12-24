package com.edureka.hbase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtility {

	private static final Logger logger = LoggerFactory.getLogger(FileUtility.class);

	public static void readFile(String filePath, TransactionRepository transactionRepository) {

		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(filePath);
			br = new BufferedReader(fr);

			String sCurrentLine;
			sCurrentLine = br.readLine();
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.replace("\"", "");
				String split[] = sCurrentLine.split(",");
				MerchantTransaction tx = new MerchantTransaction(split[2], 1l);
				transactionRepository.put(tx);
			}
		} catch (Exception e) {
			logger.error("Exception occured while reading file = {} ", e);
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException ex) {
				logger.error("Exception occured while releasing file resources = {} ", ex);
			}
		}
	}
}
