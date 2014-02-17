package com.datastax.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class ReportDao {

	private static Logger logger = LoggerFactory.getLogger(ReportDao.class);

	private Session session;
	private static String keyspaceName = "datastax_bulkload_writer_demo";
	private static String tableNameCounter = keyspaceName + ".report_counters";

	private String updateCounter = "update " + tableNameCounter
			+ " set rec_num=rec_num + ? where rundate = ? and type = ? and filename = ?";
	private String readCounter = "select rec_num from " + tableNameCounter
			+ " where rundate = ? and type = ? and filename = ?";

	private PreparedStatement updateCounterStmt;
	private PreparedStatement readCounterStmt;
	private String runDate = "20140214-" + System.currentTimeMillis();

	public ReportDao(Session session) {
		this.session = session;

		updateCounterStmt = session.prepare(updateCounter);
		readCounterStmt = session.prepare(readCounter);
	}

	public void updateCounter(String type, int increment, String fileName) {

		BoundStatement bound = new BoundStatement(updateCounterStmt);

		bound.setString("type", type);
		bound.setLong("rec_num", increment);
		bound.setString("rundate", runDate);
		bound.setString("filename", fileName);

		this.session.execute(bound);
	}

	public long readCounter(String type, String fileName) {
		BoundStatement bound = new BoundStatement(readCounterStmt);

		bound.setString("type", type);
		bound.setString("filename", fileName);
		bound.setString("rundate", runDate);

		ResultSet resultSet = this.session.execute(bound);

		if (resultSet != null) {
			Row row = resultSet.one();
			if (row != null) {
				return row.getLong("rec_num");
			}
		}

		return 0;
	}

	public synchronized String getCurrentFileName(String type, String fileName) {
		// Only one instance of ReportDao so, we have the lock so update the file name
		fileName = type + "-" + System.currentTimeMillis();
		logger.info("New file Created : " + fileName);

		return fileName;
	}
}
