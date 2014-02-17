package com.datastax.reports;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.dao.ReportDao;
import com.datastax.dao.TransactionsDao;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.model.Transaction;
import com.datastax.model.TransactionType;

public class ReportWriterImpl implements ReportWriter {

	private static int BATCH_SIZE;

	private static Logger logger = LoggerFactory.getLogger(ReportWriterImpl.class);

	private Session session;
	private TransactionsDao transactionDao;
	private ReportDao reportDao;
	private BlockingQueue<Transaction> creditCollection = new ArrayBlockingQueue<Transaction>(1000);
	private BlockingQueue<Transaction> debitCollection = new ArrayBlockingQueue<Transaction>(1000);
	private BlockingQueue<Transaction> deadLetterCollection = new ArrayBlockingQueue<Transaction>(1000);

	private String creditFileName;
	private String debitFileName;
	private String deadLetterFileName = "DL-" + System.currentTimeMillis(); 

	private BufferedWriter creditFileOut;
	private BufferedWriter debitFileOut;
	private BufferedWriter deadLetterFileOut = createOutFile(deadLetterFileName);

	private int creditCounter = 0;
	private int debitCounter = 0;

	public ReportWriterImpl() {
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		int creditThreads = Integer.parseInt(PropertyHelper.getProperty("creditThreads", "3"));
		int debitThreads = Integer.parseInt(PropertyHelper.getProperty("debitThreads", "3"));
		BATCH_SIZE = Integer.parseInt(PropertyHelper.getProperty("batchSize", "150000"));

		ExecutorService creditExecutors = Executors.newFixedThreadPool(creditThreads);
		ExecutorService debitExecutors = Executors.newFixedThreadPool(debitThreads);
		ExecutorService deadLetterExecutors = Executors.newSingleThreadExecutor();

		Cluster cluster = Cluster.builder().addContactPoints(contactPointsStr.split(",")).build();
		this.session = cluster.connect();

		this.transactionDao = new TransactionsDao(session);
		this.reportDao = new ReportDao(session);

		// Create collectors
		for (int i = 0; i < creditThreads; i++) {
			creditExecutors.execute(new CreditWriter(this.creditCollection, this));
		}
		for (int i = 0; i < creditThreads; i++) {
			debitExecutors.execute(new DebitWriter(this.debitCollection, this));
		}
		for (int i = 0; i < creditThreads; i++) {
			deadLetterExecutors.execute(new DeadLetterWriter(this.deadLetterCollection, this));
		}

		try {
			// Start processing data
			this.runReport();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		try {
			Thread.sleep(1000);
			closeAllFiles();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
			logger.warn("Problem closing files - please check : " + e.getMessage());
		}

		session.shutdown();
		cluster.shutdown();

		System.exit(0);
	}

	private void closeAllFiles() throws IOException {
		this.creditFileOut.flush();
		this.creditFileOut.close();
		this.deadLetterFileOut.flush();
		this.deadLetterFileOut.close();
		this.debitFileOut.flush();
		this.debitFileOut.close();
	}

	public void runReport() throws InterruptedException {
		this.transactionDao.getAllProducts(debitCollection, creditCollection, deadLetterCollection);
	}

	public long getTotalTransactionsProcessed() {
		return transactionDao.getTotalProducts();
	}

	public static void main(String[] args) {
		ReportWriterImpl reportWriter = new ReportWriterImpl();

		logger.info("Report Writer ran successfully with " + reportWriter.getTotalTransactionsProcessed()
				+ " processed.");

		System.exit(0);

	}

	private BufferedWriter createOutFile(String fileName) {

		File f = new File(".", "reports");
		if (!f.exists()) {
			f.mkdir();
		}

		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter("reports/" + fileName, true));
			return writer;
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	@Override
	public synchronized void writeCreditTransactionToFile(Transaction transaction) {
		if (creditCounter % BATCH_SIZE == 0 && creditCounter > 0) {
			String newCreditFileName = this.reportDao.getCurrentFileName(TransactionType.CREDIT.toString(),
					creditFileName);

			if (!newCreditFileName.equalsIgnoreCase(creditFileName)) {

				try {
					flushAndCloseFile(creditFileOut);
				} catch (IOException e) {
					e.printStackTrace();
				}

				creditFileOut = createOutFile(newCreditFileName);
			}
		}

		try {
			this.writeToFile(transaction, this.creditFileOut);
			creditCounter++;
		} catch (IOException e) {
			logger.warn("Could not write credit transaction : " + e.getMessage());
			this.writeDeadLetterTransactionToFile(transaction);
		}
	}

	@Override
	public synchronized void writeDebitTransactionToFile(Transaction transaction) {
		if (debitCounter % BATCH_SIZE == 0 && debitCounter > 0) {
			String newDebitFileName = this.reportDao
					.getCurrentFileName(TransactionType.DEBIT.toString(), debitFileName);

			if (!newDebitFileName.equalsIgnoreCase(debitFileName)) {
				// Close old file
				try {
					flushAndCloseFile(debitFileOut);
				} catch (IOException e) {
					e.printStackTrace();
				}
				debitFileOut = createOutFile(newDebitFileName);
			}
		}

		try {
			this.writeToFile(transaction, this.debitFileOut);
			debitCounter++;
		} catch (IOException e) {
			logger.warn("Could not write debit transaction : " + e.getMessage());
			this.writeDeadLetterTransactionToFile(transaction);
		}
	}

	@Override
	public synchronized void writeDeadLetterTransactionToFile(Transaction transaction) {
		try {
			this.writeToFile(transaction, this.deadLetterFileOut);
		} catch (IOException e) {
			logger.warn("Could not write debit transaction : " + e.getMessage());
			this.writeDeadLetterTransactionToFile(transaction);
		}
	}

	public void writeToFile(Transaction transaction, BufferedWriter out) throws IOException {
		try {
			if (out != null) {
				out.write(transaction.toCVSString() + "\n");
			}
		} catch (IOException e) {
			throw e;
		}
	}

	public void flushAndCloseFile(BufferedWriter out) throws IOException {
		try {
			if (out != null) {
				out.flush();
				out.close();
			}
		} catch (IOException e) {
			throw e;
		}
	}
}
