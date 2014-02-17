package com.datastax.reports;

import com.datastax.model.Transaction;

public interface ReportWriter {

	public void writeCreditTransactionToFile(Transaction transaction);
	
	public void writeDebitTransactionToFile(Transaction transaction);
	
	public void writeDeadLetterTransactionToFile(Transaction transaction);
	
}
