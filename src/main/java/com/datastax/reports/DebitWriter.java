package com.datastax.reports;

import java.util.concurrent.BlockingQueue;

import com.datastax.model.Transaction;

public class DebitWriter implements Runnable {

	private BlockingQueue<Transaction> queue;
	private ReportWriter reportWriter;

	public DebitWriter(BlockingQueue<Transaction> queue, ReportWriter reportWriter) {
		this.queue = queue;
		this.reportWriter = reportWriter;
	}

	@Override
	public void run() {			
		while(true){				
			Transaction transaction = queue.poll();
			
			if (transaction!=null){
				reportWriter.writeDebitTransactionToFile(transaction);
			}				
		}				
	}
}
