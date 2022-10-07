package com.cic.bd.test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
 * 检查目标表字段名，字段顺序，字段类型
 * @author chen-
 *
 */
public class TCLanding04 {
	private static Logger logger = Logger.getLogger(TCLanding04.class);
	private static Logger tcLanding04Logger = Logger.getLogger("tcLanding04");

	private static final String TEST_CASE_NAME = "TCLanding04";
	private static final int TABLE_NAME_INDEX = 1;
	private static final int COLUMN_ORDER_INDEX = 2;
	private static final int COLUMN_NAME_INDEX = 3;
	private static final int COLUMN_TYPE_INDEX = 5;

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
			"tcLanding04InputFile" })
	public void test(String krb5Conf, String kerberosUser, String keytabPath, String databaseType, String url,
			String username, String password, String tcLanding04InputFile) throws Exception {
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
		Map<String, List<CSVRecord>> expectedTableColumns = new TreeMap<String, List<CSVRecord>>();

		CSVFormat csvFormat = CSVFormat.newFormat(',');
		csvFormat = csvFormat.withSkipHeaderRecord(false);
		csvFormat = csvFormat.withIgnoreEmptyLines(true);
		csvFormat = csvFormat.withQuote('"');

		try (FileInputStream fis = new FileInputStream(tcLanding04InputFile);
				InputStreamReader isr = new InputStreamReader(fis, "utf-8");
				CSVParser csvParser = new CSVParser(isr, csvFormat);) {
			Consumer<CSVRecord> consumer = new Consumer<CSVRecord>() {
				public void accept(CSVRecord record) {

					logger.debug(TEST_CASE_NAME + " handle line:" + record.getRecordNumber() + ", record:"
							+ record.toString());

					String tableName = record.get(TABLE_NAME_INDEX);

					List<CSVRecord> columnList = expectedTableColumns.get(tableName);
					if (columnList == null) {
						columnList = new ArrayList<CSVRecord>();
						columnList.add(record);

						expectedTableColumns.put(tableName, columnList);
					} else {
						columnList.add(record);
					}
				}
			};
			csvParser.forEach(consumer);

			// This to make sure that all columns are sorted by column order
			for (Map.Entry<String, List<CSVRecord>> entry : expectedTableColumns.entrySet()) {
				entry.getValue().sort(new Comparator<CSVRecord>() {
					public int compare(CSVRecord record1, CSVRecord record2) {
						int columOrder1 = Integer.parseInt(record1.get(COLUMN_ORDER_INDEX));
						int columOrder2 = Integer.parseInt(record2.get(COLUMN_ORDER_INDEX));

						if (columOrder1 > columOrder2) {
							return 1;
						} else {
							return -1;
						}
					}
				});

			}
		}
		logger.info(TEST_CASE_NAME + " end read input file");
		logger.info(TEST_CASE_NAME + " expected tables count:" + expectedTableColumns.size());

		logger.debug(TEST_CASE_NAME + " kerberos login with krb5Conf:" + krb5Conf + ", kerberosUser:" + kerberosUser
				+ ", keytabPath:" + keytabPath);
		HiveConnection.kerberosLogin(krb5Conf, kerberosUser, keytabPath);
		logger.debug(TEST_CASE_NAME + " kerberos login successed");

		logger.debug(TEST_CASE_NAME + " connect to hive with url:" + url + ",username:" + username + ",password:"
				+ password);
		
		List<String> failedList = new ArrayList<String>();
		try (Connection conn = HiveConnection.getConnection(url, username, password);) {
			logger.debug(TEST_CASE_NAME + " connect to hive successed");
			for (Map.Entry<String, List<CSVRecord>> entry : expectedTableColumns.entrySet()) {
				List<String> messages = new ArrayList<String>();

				String tableName = entry.getKey();
				logger.debug(TEST_CASE_NAME + " start check:" + tableName);

				logger.debug(TEST_CASE_NAME + " start read columns for table:" + tableName);
				try (Statement columnStmt = conn.createStatement();
						ResultSet columnResultSet = columnStmt.executeQuery("desc " + tableName)) {
					logger.debug(TEST_CASE_NAME + " end read columns for table:" + tableName);

					List<CSVRecord> expectedColumns = entry.getValue();
					for (CSVRecord csvRecord : expectedColumns) {
						String expectedColName = csvRecord.get(COLUMN_NAME_INDEX);
						String expectedType = csvRecord.get(COLUMN_TYPE_INDEX); 

						if (!columnResultSet.next()) {
							messages.add(expectedColName + " does not exist.");
							break;
						}

						String realColName = columnResultSet.getString("col_name");

						if("processing_dttm".equals(realColName)) {
							columnResultSet.next();
							realColName = columnResultSet.getString("col_name");
						}

						String realType = columnResultSet.getString("data_type");

						if (!expectedColName.toUpperCase().equals(realColName.toUpperCase())) {
							messages.add(tableName + " column name doesn't match:" + expectedColName + "," + realColName);
						}

						if (!expectedType.toUpperCase().equals(realType.toUpperCase())) {
							messages.add(tableName + " column type doesn't match:" + expectedColName + "." + expectedType + "," + realColName + "." + realType);
						}
					}

					if (!columnResultSet.next()) {
						String realColName = columnResultSet.getString("col_name");
						if(!"processing_dttm".equals(realColName)) {
							messages.add(tableName + " with extra column:" + realColName);
						}
					}
				} catch(Exception e) {
					messages.add(tableName + " with exception:" + e.getMessage());
				}

				if (messages.isEmpty()) {
					tcLanding04Logger.info(tableName + "正确性检查-列名/列顺序/列类型" + "," + "Passed");
				} else {
					tcLanding04Logger.info(tableName + "正确性检查-列名/列顺序/列类型" + "," + "Failed");
				}

				failedList.addAll(messages);
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