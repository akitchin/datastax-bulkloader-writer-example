package com.datastax.reports;

import java.util.concurrent.BlockingQueue;

import com.datastax.model.Transaction;

public class DeadLetterWriter implements Runnable {

	private BlockingQueue<Transaction> queue;
	private ReportWriter reportWriter;

	public DeadLetterWriter(BlockingQueue<Transaction> queue, ReportWriter reportWriter) {
		this.queue = queue;
		this.reportWriter = reportWriter;
	}

	@Override
	public void run() {			
		while(true){				
			Transaction transaction = queue.poll();
			
			if (transaction!=null){
				this.reportWriter.writeDeadLetterTransactionToFile(transaction);
			}				
		}				
	}
}
