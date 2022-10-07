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

public class TCLanding07 {
	private static Logger logger = Logger.getLogger(TCLanding07.class);
	private static Logger tcLanding07Logger = Logger.getLogger("tcLanding07");
	
	private static final String TEST_CASE_NAME = "TCLanding07";
	private static final int TABLE_NAME_INDEX = 2;

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
			"tcLanding07InputFile" })
	public void test(String krb5Conf, String kerberosUser, String keytabPath, String databaseType, String url,
			String username, String password, String tcLanding07InputFile) throws Exception {
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

		try (FileInputStream fis = new FileInputStream(tcLanding07InputFile);
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
				Statement distinctStmt = conn.createStatement();
				Statement nonDistinctStmt = conn.createStatement();
				) {
			for (CSVRecord csvRecord : csvRecords) {
				String tableName = csvRecord.get(TABLE_NAME_INDEX);
				
				logger.debug(TEST_CASE_NAME + " start check for " + tableName);
				
				String expectedSql = "select count(1) from (select distinct * from " +  tableName + ") a";
				String realSql = "select count(1) from " + tableName;
				
				logger.debug(TEST_CASE_NAME + " " + tableName + " expectedSql:" + expectedSql);
				logger.debug(TEST_CASE_NAME + " " + tableName + " realSql:" + realSql);

				List<String> messages = new ArrayList<String>();
				try(ResultSet distinctResultSet = distinctStmt.executeQuery(expectedSql);
						ResultSet nonDistinctResultSet = nonDistinctStmt.executeQuery(realSql);) {
					
					if (!distinctResultSet.next()) {
						logger.error(TEST_CASE_NAME + " No distinct for table:" + tableName);
						tcLanding07Logger.info(tableName + "完整性检查-记录数" + "," + "No Run");
						continue;
					}
					
					if (!nonDistinctResultSet.next()) {
						logger.error(TEST_CASE_NAME + " No non-distinct result for table:" + tableName);
						tcLanding07Logger.info(tableName + "完整性检查-记录数" + "," + "No Run");
						continue;
					}

					int distinctCount = distinctResultSet.getInt(1);
					int nonDistinctCount = nonDistinctResultSet.getInt(1);

					if(distinctCount != nonDistinctCount) {
						messages.add("check duplicate record for table " + tableName + ", distinct:" + distinctCount + ",nonDistinct:" + nonDistinctCount);
					}				
				} catch (Exception e) {
					messages.add(tableName + " with exception:" + e.getMessage());
				}

				if (messages.isEmpty()) {
					tcLanding07Logger.info(tableName + "重复性检查-数据无重复" + "," + "Passed");
				} else {
					tcLanding07Logger.info(tableName + "重复性检查-数据无重复" + "," + "Failed");
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