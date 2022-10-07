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

public class TCLanding05 {
	private static Logger logger = Logger.getLogger(TCLanding05.class);
	private static Logger tcLanding05Logger = Logger.getLogger("tcLanding05");

	private static final String TEST_CASE_NAME = "TCLanding05";
	private static final int SQL_TABLE_NAME_INDEX = 2;
	private static final int SQL_INDEX = 6;

	private static final int DIC_TABLE_NAME_INDEX = 1;
	private static final int DIC_COLUMN_ORDER_INDEX = 2;
	private static final int DIC_COLUNN_NAME = 3;
	private static final int DIC_COLUNN_TYPE = 5;

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
			"tcLanding05InputFile01", "tcLanding05InputFile02"})
	public void test(String krb5Conf, String kerberosUser, String keytabPath, String databaseType, String url,
			String username, String password, String tcLanding05InputFile01, String tcLanding05InputFile02) throws Exception {
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
		logger.info(TEST_CASE_NAME + " start read input file (SQL)");

		List<CSVRecord> csvRecords = new ArrayList<CSVRecord>();

		CSVFormat csvFormat = CSVFormat.newFormat(',');
		csvFormat = csvFormat.withSkipHeaderRecord(false);
		csvFormat = csvFormat.withIgnoreEmptyLines(true);
		csvFormat = csvFormat.withQuote('"');

		try (FileInputStream fis = new FileInputStream(tcLanding05InputFile01);
				InputStreamReader isr = new InputStreamReader(fis, "utf-8");
				CSVParser csvParser = new CSVParser(isr, csvFormat);) {
			Consumer<CSVRecord> consumer = new Consumer<CSVRecord>() {
				public void accept(CSVRecord record) {
					logger.debug(TEST_CASE_NAME + " handle sql line:" + record.getRecordNumber() + ", record:"
							+ record.toString());
					csvRecords.add(record);
				}
			};
			csvParser.forEach(consumer);
		}
		logger.info(TEST_CASE_NAME + " end read input file(SQL)");
		
		
		logger.info(TEST_CASE_NAME + " start read input file (dicitonary)");
		Map<String, List<CSVRecord>> tableColumns = new TreeMap<String, List<CSVRecord>>();
		try (FileInputStream fis = new FileInputStream(tcLanding05InputFile02);
				InputStreamReader isr = new InputStreamReader(fis, "utf-8");
				CSVParser csvParser = new CSVParser(isr, csvFormat);) {
			Consumer<CSVRecord> consumer = new Consumer<CSVRecord>() {
				public void accept(CSVRecord record) {
					logger.debug(TEST_CASE_NAME + " handle dic line:" + record.getRecordNumber() + ", record:"
							+ record.toString());

					String tableName = record.get(DIC_TABLE_NAME_INDEX);

					List<CSVRecord> columnList = tableColumns.get(tableName);
					if (columnList == null) {
						columnList = new ArrayList<CSVRecord>();
						columnList.add(record);

						tableColumns.put(tableName, columnList);
					} else {
						columnList.add(record);
					}
				}
			};
			csvParser.forEach(consumer);

			// This to make sure that all columns are sorted by column order
			for (Map.Entry<String, List<CSVRecord>> entry : tableColumns.entrySet()) {
				entry.getValue().sort(new Comparator<CSVRecord>() {
					public int compare(CSVRecord record1, CSVRecord record2) {
						int columOrder1 = Integer.parseInt(record1.get(DIC_COLUMN_ORDER_INDEX));
						int columOrder2 = Integer.parseInt(record2.get(DIC_COLUMN_ORDER_INDEX));

						if (columOrder1 > columOrder2) {
							return 1;
						} else {
							return -1;
						}
					}
				});
			}
		}
		logger.info(TEST_CASE_NAME + " end read input file(dicitonary)");

		logger.debug(TEST_CASE_NAME + " kerberos login with krb5Conf:" + krb5Conf + ", kerberosUser:" + kerberosUser
				+ ", keytabPath:" + keytabPath);
		HiveConnection.kerberosLogin(krb5Conf, kerberosUser, keytabPath);
		logger.debug(TEST_CASE_NAME + " kerberos login successed");
		
		List<String> failedList = new ArrayList<String>();

		try(Connection conn = HiveConnection.getConnection(url, username, password);
				Statement expectedStmt = conn.createStatement();
				Statement realStmt = conn.createStatement();) {
			for (CSVRecord sqlCSVRecord : csvRecords) {
				
				List<String> messages = new ArrayList<String>();

				String tableName = sqlCSVRecord.get(SQL_TABLE_NAME_INDEX);
				logger.debug(TEST_CASE_NAME + " start check for " + tableName);
				List<CSVRecord> columList = tableColumns.get(tableName);

				if (columList == null || columList.size() == 0) {
					logger.warn(TEST_CASE_NAME + " column list is not found in dictionary");
					tcLanding05Logger.info(tableName + "正确性检查-数据值" + "," + "No Run");
					continue;
				}

				String originalSQL = sqlCSVRecord.get(SQL_INDEX).trim();
				if (originalSQL.endsWith(";")) {
					originalSQL = originalSQL.substring(0, originalSQL.length() - 1);
				}

				String sqlForamt = "select md5(concat_ws(',', collect_set(c.col1))) from (" +
						"select b.col1 from(" +
						"select %s col1 from %s a) b order by b.col1) c";
				
				for (CSVRecord columnCSVRecord: columList) {
					String columnName = columnCSVRecord.get(DIC_COLUNN_NAME);
					String columnType = columnCSVRecord.get(DIC_COLUNN_TYPE);

					String expectedColumnSQL = getExpectedColumnSQL(columnName, columnType);
					String realColumnSQL = getRealColumnSQL(columnName, columnType);

					String expectedSQL = String.format(sqlForamt, expectedColumnSQL, "(" + originalSQL + ")");
					String realSQL = String.format(sqlForamt, realColumnSQL, tableName);

					logger.debug(TEST_CASE_NAME + " " + tableName + " expectedSql:" + expectedSQL);
					logger.debug(TEST_CASE_NAME + " " + tableName + " realSql:" + realSQL);
					
					try(ResultSet expectedResultSet = expectedStmt.executeQuery(expectedSQL);
							ResultSet realResultSet = realStmt.executeQuery(realSQL)) {
						if (!expectedResultSet.next()) {
							logger.error(TEST_CASE_NAME + " No expted result for table:" + tableName);
							tcLanding05Logger.info(tableName + "完整性检查-记录数" + "," + "No Run");
							continue;
						}
						
						if (!realResultSet.next()) {
							logger.error(TEST_CASE_NAME + " No real result for table:" + tableName);
							tcLanding05Logger.info(tableName + "完整性检查-记录数" + "," + "No Run");
							continue;
						}

						String expectedValue = expectedResultSet.getString(1);
						logger.debug(TEST_CASE_NAME + " expectedValue for " + tableName + "." + columnName + " is " + expectedValue);

						String realValue = realResultSet.getString(1);
						logger.debug(TEST_CASE_NAME + " realValue for " + tableName + "." + columnName + " is " + realValue);

						if (!expectedValue.equals(realValue)) {
							messages.add("check for " + tableName + "." + columnName + " failed, expectedValue:" + expectedValue + ",realValue:" + realValue);
							logger.debug(TEST_CASE_NAME + " check for " + tableName + "." + columnName + " failed, expectedValue:" + expectedValue + ",realValue:" + realValue);
						}
					} catch(Exception e) {
						messages.add(tableName + " with exception:" + e.getMessage());
						logger.debug(TEST_CASE_NAME + " " + tableName + " with exception:" + e.getMessage());
						
						// if an exception throw here, it will goto next table
						// let's pray to god it will work fine :)
						break;
					}
				}

				if (messages.isEmpty()) {
					tcLanding05Logger.info(tableName + "正确性检查-数据值" + "," + "Passed");
				} else {
					tcLanding05Logger.info(tableName + "正确性检查-数据值" + "," + "Failed");
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

	private String getExpectedColumnSQL(String columnName, String columnType) {
		String standardColumnName = "`" + columnName + "`";
		
		if (columnType.equals("string")) {
			//return "nvl(regexp_replace(a." + standardColumnName + ", '[ \\t\\r\\n]*([^ \\t\\r\\n].*[^ \\t\\r\\n])[ \\t\\r\\n]*','$1'),'')";
			//return "nvl(regexp_replace(a." + standardColumnName + ", '[\\\\s]*([^\\\\s]*.*[^\\\\s]*)[\\\\s]*','$1'),'')";
			return "nvl(regexp_replace(a." + standardColumnName + ", '^\\\\s+|\\\\s+$',''),'')";

			//return "nvl(trim(a." + standardColumnName + "),'')";
		}
		
		if (columnType.equals("date")) {
			return "nvl(cast(a." + standardColumnName + " as string), '1970-01-01')";
		}
		if (columnType.equals("timestamp")) {
			return "date_format(nvl(a." + standardColumnName + ", cast('1970-01-01 00:00:00.0' as timestamp)), 'yyyy-MM-dd HH:mm:ss.S')";
		}

		return "cast(a." + standardColumnName + " as string)";
	}

	private String getRealColumnSQL(String columnName, String columnType) {
		String standardColumnName = "`" + columnName + "`";

		if (columnType.equals("string")) {
			return "a." + standardColumnName;
		}
		if (columnType.equals("timestamp")) {
			return "date_format(a." + standardColumnName + ",'yyyy-MM-dd HH:mm:ss.S')";
		}

		return "cast(a." + standardColumnName + " as string)";
	}
}
