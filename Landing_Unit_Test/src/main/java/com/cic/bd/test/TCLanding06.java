package com.cic.bd.test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import comc.cic.bd.test.util.HiveConnection;

/**
 * ���SQLִ�н��(count)��Ŀ���(count)һ��
 * 
 * @author chen-
 *
 */
public class TCLanding06 {
	private static Logger logger = Logger.getLogger(TCLanding06.class);
	private static Logger tcLanding06Logger = Logger.getLogger("tcLanding06");

	private static final String TEST_CASE_NAME = "TCLanding06";
	private static final int TABLE_NAME_INDEX = 2;
	private static final int SQL_INDEX = 6;

	@BeforeClass
	@Parameters({ "krb5Conf", "kerberosUser", "keytabPath", "databaseType", "url", "username", "password" })
	public void setup(String krb5Conf, String kerberosUser, String keytabPath, String databaseType, String url,
			String username, String password) throws Exception {
		/*
		 * logger.info(TEST_CASE_NAME + "setup start delete tables");
		 * 
		 * logger.debug(TEST_CASE_NAME + " kerberos login with krb5Conf:" +
		 * krb5Conf + ", kerberosUser:" + kerberosUser + ", keytabPath:" +
		 * keytabPath); HiveConnection.kerberosLogin(krb5Conf, kerberosUser,
		 * keytabPath); logger.debug(TEST_CASE_NAME +
		 * " kerberos login successed");
		 * 
		 * logger.debug(TEST_CASE_NAME + " connect to hive with url:" + url +
		 * ",username:" + username + ",password:" + password); try (Connection
		 * conn = HiveConnection.getConnection(url, username, password);) {
		 * logger.debug(TEST_CASE_NAME + " connect to hive successed");
		 * 
		 * try (Statement tableStmt = conn.createStatement(); ResultSet
		 * tableResultSet = tableStmt.executeQuery("show tables");) {
		 * 
		 * Statement dropStmt = conn.createStatement();
		 * while(tableResultSet.next()) { String tableName =
		 * tableResultSet.getString("tab_name"); logger.debug(TEST_CASE_NAME +
		 * " delete table " + tableName);
		 * 
		 * dropStmt.execute("drop table " + tableName); } } }
		 * logger.info(TEST_CASE_NAME + " setup end delete tables");
		 */
	}

	@Test
	@Parameters({ "krb5Conf", "kerberosUser", "keytabPath", "databaseType", "url", "username", "password",
			"tcLanding06InputFile" })
	public void test(String krb5Conf, String kerberosUser, String keytabPath, String databaseType, String url,
			String username, String password, String tcLanding06InputFile) throws Exception {
		/*
		 * JerseyClientConfig jerseyClientConfig = new JerseyClientConfig();
		 * jerseyClientConfig.setHost("");
		 * jerseyClientConfig.setPort(Integer.parseInt(""));
		 * jerseyClientConfig.setUsername("");
		 * jerseyClientConfig.setPassword("".toCharArray()); JerseyRestClient
		 * jerseyRestClient = new JerseyRestClient(jerseyClientConfig);
		 * 
		 * String feedListUrl = "/v1/feedmgr/feeds";
		 */

		logger.info(TEST_CASE_NAME + " start");
		logger.info(TEST_CASE_NAME + " start read input file");
		
		List<CSVRecord> csvRecords = new ArrayList<CSVRecord>();

		CSVFormat csvFormat = CSVFormat.newFormat(',');
		csvFormat = csvFormat.withSkipHeaderRecord(false);
		csvFormat = csvFormat.withIgnoreEmptyLines(true);
		csvFormat = csvFormat.withQuote('"');

		try (FileInputStream fis = new FileInputStream(tcLanding06InputFile);
				InputStreamReader isr = new InputStreamReader(fis, "utf-8");
				CSVParser csvParser = new CSVParser(isr, csvFormat);) {
			Consumer<CSVRecord> consumer = new Consumer<CSVRecord>() {
				public void accept(CSVRecord record) {

					logger.debug(TEST_CASE_NAME + " handle line:" + record.getRecordNumber() + ", record:"
							+ record.toString());

					csvRecords.add(record);
				}
			};
			csvParser.forEach(consumer);
		}
		logger.info(TEST_CASE_NAME + " end read input file");
		
		logger.debug(TEST_CASE_NAME + " kerberos login with krb5Conf:" + krb5Conf + ", kerberosUser:" + kerberosUser
				+ ", keytabPath:" + keytabPath);
		HiveConnection.kerberosLogin(krb5Conf, kerberosUser, keytabPath);
		logger.debug(TEST_CASE_NAME + " kerberos login successed");
		
		List<String> failedList = new ArrayList<String>();

		try(Connection conn = HiveConnection.getConnection(url, username, password);
				Statement expectedStmt = conn.createStatement();
				Statement realStmt = conn.createStatement();
				) {
			for (CSVRecord csvRecord : csvRecords) {
				String tableName = csvRecord.get(TABLE_NAME_INDEX);
				
				List<String> messages = new ArrayList<String>();

				logger.debug(TEST_CASE_NAME + " start check for " + tableName);
				
				String expectedSql = csvRecord.get(SQL_INDEX).trim();
				if (expectedSql.endsWith(";")) {
					expectedSql = expectedSql.substring(0, expectedSql.length() - 1);
				}
				String newExpectedSql = "select count(1) from (" + expectedSql + ") a";
				String realSql = "select count(1) from " + tableName;

				logger.debug(TEST_CASE_NAME + " " + tableName + " expectedSql:" + newExpectedSql);
				logger.debug(TEST_CASE_NAME + " " + tableName + " realSql:" + realSql);

				int expectedCount = 0;
				int realCount = 0;
				try(ResultSet expectedResultSet = expectedStmt.executeQuery(newExpectedSql);
						ResultSet realResultSet = realStmt.executeQuery(realSql);) {
					
					if (!expectedResultSet.next()) {
						logger.error(TEST_CASE_NAME + " No expted result for table:" + tableName);
						tcLanding06Logger.info(tableName + "完整性检查-记录数" + "," + "No Run");
						continue;
					}
					
					if (!realResultSet.next()) {
						logger.error(TEST_CASE_NAME + " No real result for table:" + tableName);
						tcLanding06Logger.info(tableName + "完整性检查-记录数" + "," + "No Run");
						continue;
					}

					expectedCount = expectedResultSet.getInt(1);
					logger.debug(TEST_CASE_NAME + " expectedCount for " + tableName + " is " + expectedCount);

					realCount = realResultSet.getInt(1);
					logger.debug(TEST_CASE_NAME + " realCount for " + tableName + " is " + expectedCount);

					if (expectedCount != realCount) {
						messages.add(TEST_CASE_NAME + " check count for " + tableName + " failed, expectedCount:" + expectedCount + ",realCount:" + realCount);
					}
				} catch(Exception e) {
					messages.add(tableName + " with exception:" + e.getMessage());
				}

				if (messages.isEmpty()) {
					if (expectedCount == 0) {
						tcLanding06Logger.info(tableName + "完整性检查-记录数,Passed,空表");
					} else {
						tcLanding06Logger.info(tableName + "完整性检查-记录数,Passed,非空表");
					}
					
				} else {
					if (expectedCount == 0) {
						tcLanding06Logger.info(tableName + "完整性检查-记录数,Failed,空表");
					} else {
						tcLanding06Logger.info(tableName + "完整性检查-记录数,Failed,非空表");
					}
					
				}

				failedList.addAll(messages);

				logger.debug(TEST_CASE_NAME + " end check for " + tableName);
			}
		}

		StringBuffer messageBuffer = new StringBuffer();
		for (String failedMessage : failedList) {
			messageBuffer.append(failedMessage + "\r\n");
		}
		Assert.assertTrue(failedList.isEmpty(), messageBuffer.toString());
		
		logger.info(TEST_CASE_NAME + " end");
	}
}