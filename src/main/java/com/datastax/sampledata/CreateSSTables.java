package com.datastax.sampledata;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bulkloader.BulkLoader;
import com.datastax.model.Transaction;

public class CreateSSTables {

	private static Logger logger = LoggerFactory.getLogger(CreateSSTables.class);
	
	public static void createSSTables(BulkLoader bulkLoader, int totalTrans) throws IOException {
				
		int batch = 10000;
		int cycles = totalTrans / batch;	
				
		for (int i=0; i < cycles; i++){
			List<Transaction> transactions = TransactionGenerator.generatorTransaction(batch);
			bulkLoader.loadTransactions(transactions);
			
			if (cycles % batch == 0){
				logger.debug("Wrote " + i + " of " + cycles + " cycles. Batch size : " + batch); 			
			}
		}				
		bulkLoader.finish();
		
		logger.debug("Finished file with " + totalTrans + " transactions.");
	}

}
