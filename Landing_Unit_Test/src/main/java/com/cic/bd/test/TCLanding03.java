package com.cic.bd.test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
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
 * ���Ŀ���
 * @author chen-
 *
 */
public class TCLanding03 {
	private static Logger logger = Logger.getLogger(TCLanding03.class);
	private static Logger tcLanding03Logger = Logger.getLogger("tcLanding03");

	private static final String TEST_CASE_NAME = "TCLanding03";
	private static final int TABLE_NAME_INDEX = 1;

	@BeforeClass
	@Parameters({ "krb5Conf", "kerberosUser", "keytabPath", "databaseType", "url", "username", "password"})
	public void setup(String krb5Conf, String kerberosUser, String keytabPath, String databaseType, String url,
			String username, String password) throws Exception {
		/*logger.info(TEST_CASE_NAME + "setup start delete tables");
		
		logger.debug(TEST_CASE_NAME + " kerberos login with krb5Conf:" + krb5Conf + ", kerberosUser:" + kerberosUser
				+ ", keytabPath:" + keytabPath);
		HiveConnection.kerberosLogin(krb5Conf, kerberosUser, keytabPath);
		logger.debug(TEST_CASE_NAME + " kerberos login successed");

		logger.debug(TEST_CASE_NAME + " connect to hive with url:" + url + ",username:" + username + ",password:"
				+ password);
		try (Connection conn = HiveConnection.getConnection(url, username, password);) {
			logger.debug(TEST_CASE_NAME + " connect to hive successed");

			try (Statement tableStmt = conn.createStatement();
				ResultSet tableResultSet = tableStmt.executeQuery("show tables");) {

				Statement dropStmt = conn.createStatement();
				while(tableResultSet.next()) {
					String tableName = tableResultSet.getString("tab_name");
					logger.debug(TEST_CASE_NAME + " delete table " + tableName);
					
					dropStmt.execute("drop table " + tableName);
				}
			}
		}
		logger.info(TEST_CASE_NAME + " setup end delete tables");*/
	}
	
	
	@Test
	@Parameters({ "krb5Conf", "kerberosUser", "keytabPath", "databaseType", "url", "username", "password",
			"tcLanding03InputFile" })
	public void test(String krb5Conf, String kerberosUser, String keytabPath, String databaseType, String url,
			String username, String password, String tcLanding03InputFile) throws Exception {
		/*
		JerseyClientConfig jerseyClientConfig = new JerseyClientConfig();
		jerseyClientConfig.setHost("");
		jerseyClientConfig.setPort(Integer.parseInt(""));
		jerseyClientConfig.setUsername("");
		jerseyClientConfig.setPassword("".toCharArray());
		JerseyRestClient jerseyRestClient = new JerseyRestClient(jerseyClientConfig);

		String feedListUrl = "/v1/feedmgr/feeds";
		*/

		logger.info(TEST_CASE_NAME + " start");
		logger.info(TEST_CASE_NAME + " start read input file");
		Set<String> expectedTableNames = new TreeSet<String>();

		CSVFormat csvFormat = CSVFormat.newFormat(',');
		csvFormat = csvFormat.withSkipHeaderRecord(false);
		csvFormat = csvFormat.withIgnoreEmptyLines(true);
		csvFormat = csvFormat.withQuote('"');

		try (FileInputStream fis = new FileInputStream(tcLanding03InputFile);
				InputStreamReader isr = new InputStreamReader(fis, "utf-8");
				CSVParser csvParser = new CSVParser(isr, csvFormat);) {
			Consumer<CSVRecord> consumer = new Consumer<CSVRecord>() {
				public void accept(CSVRecord record) {

					logger.debug(TEST_CASE_NAME + " handle line:" + record.getRecordNumber() + ", record:"
							+ record.toString());

					String tableName = record.get(TABLE_NAME_INDEX);

					expectedTableNames.add(tableName);
				}
			};
			csvParser.forEach(consumer);
		}

		logger.info(TEST_CASE_NAME + " end read input file");

		logger.info(TEST_CASE_NAME + " expected tables count:" + expectedTableNames.size());
		

		logger.info(TEST_CASE_NAME + " start read database schema");

		logger.debug(TEST_CASE_NAME + " kerberos login with krb5Conf:" + krb5Conf + ", kerberosUser:" + kerberosUser
				+ ", keytabPath:" + keytabPath);
		HiveConnection.kerberosLogin(krb5Conf, kerberosUser, keytabPath);
		logger.debug(TEST_CASE_NAME + " kerberos login successed");

		logger.debug(TEST_CASE_NAME + " connect to hive with url:" + url + ",username:" + username + ",password:"
				+ password);
		Set<String> realTables = new TreeSet<String>();
		try (Connection conn = HiveConnection.getConnection(url, username, password);) {
			logger.debug(TEST_CASE_NAME + " connect to hive successed");

			logger.debug(TEST_CASE_NAME + " start read tables");
			try (Statement tableStmt = conn.createStatement();
				ResultSet tableResultSet = tableStmt.executeQuery("show tables");) {
				logger.debug(TEST_CASE_NAME + " end read tables");

				while (tableResultSet.next()) {
					String tableName = tableResultSet.getString("tab_name").toUpperCase();
					logger.debug(TEST_CASE_NAME + " start check:" + tableName);

					// these tables are creaed by kylo for temporary process
					if (tableName.endsWith("_feed") || tableName.endsWith("_invalid") || tableName.endsWith("_profile") || tableName.endsWith("_valid")) {
						continue;
					}
					realTables.add(tableName);
				}
			}
		}

		List<String> failedList = new ArrayList<String>();
		for(String expectedTableName : expectedTableNames) {
			if (realTables.contains(expectedTableName.toUpperCase())) {
				tcLanding03Logger.info(expectedTableName + "正确性检查-表名" + "," + "Passed");
			} else {
				failedList.add(expectedTableName);
				tcLanding03Logger.info(expectedTableName + "正确性检查-表名" + "," + "Failed");
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
