package com.datastax.dao;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.model.Transaction;
import com.datastax.model.TransactionType;

public class TransactionsDao {

	private static Logger logger = LoggerFactory.getLogger(TransactionsDao.class);

	private Session session;
	private static AtomicLong TOTAL_PRODUCTS = new AtomicLong(0);

	private static String keyspaceName = "datastax_bulkload_writer_demo";
	private static String tableNameProduct = keyspaceName + ".transactions";

	private static final String SELECT_ALL_PRODUCTS = "Select * from " + tableNameProduct;

	private PreparedStatement selectStmtProduct;

	public TransactionsDao(Session session) {
		this.session = session;
		
		this.selectStmtProduct = session.prepare(SELECT_ALL_PRODUCTS);
		this.selectStmtProduct.setConsistencyLevel(ConsistencyLevel.QUORUM);
	}

	public void getAllProducts(BlockingQueue<Transaction> debitCollection, BlockingQueue<Transaction> creditCollection,
			BlockingQueue<Transaction> deadLetterCollection) throws InterruptedException {

		Statement stmt = new SimpleStatement("Select * from " + tableNameProduct);
		stmt.setFetchSize(500);
		ResultSet resultSet = this.session.execute(stmt);

		Iterator<Row> iterator = resultSet.iterator();

		while (iterator.hasNext()) {

			Row row = iterator.next();

			Transaction transaction = createTransactionFromRow(row);
			
			try {
				if (transaction.getType().equalsIgnoreCase(TransactionType.DEBIT.getValue())) {
					debitCollection.put(transaction);

				} else if (transaction.getType().equalsIgnoreCase(TransactionType.CREDIT.getValue())) {
					creditCollection.put(transaction);
				}
			} catch (InterruptedException e) {
				logger.warn("Could not process transaction due to error : " + e.getMessage());
				
				try {
					deadLetterCollection.put(transaction);
				} catch (InterruptedException ie) {
					logger.error("Could not process add transaction to dead letter queue due to error : " + e.getMessage());
					throw ie;
				}
			}
			TOTAL_PRODUCTS.incrementAndGet();
		}
	}

	private Transaction createTransactionFromRow(Row row) {
		Transaction trans = new Transaction();
		trans.setAcountId(row.getString("accid"));
		trans.setAmount(row.getDouble("amount"));
		trans.setTxtnId(row.getUUID("txtnid").toString());
		trans.setTxtnDate(row.getDate("txtntime"));
		trans.setReason(row.getString("reason"));
		trans.setType(row.getString("type"));

		return trans;
	}

	public long getTotalProducts() {
		return TOTAL_PRODUCTS.longValue();
	}
}
