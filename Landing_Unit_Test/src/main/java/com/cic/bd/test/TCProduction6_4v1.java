package com.cic.bd.test;

import com.cic.bd.test.util.HiveConnectionUtil;
import com.cic.bd.test.util.VeticaConnectionUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

public class TCProduction6_4 {
    private static Logger TCRunLog = Logger.getLogger(TCProduction6_4.class);
    private static Logger TCResult = Logger.getLogger("TCProduction6_4");
    private static Logger TCResultSummary = Logger.getLogger("TCProduction6_4Sum");
    private static final String TEST_CASE = "TCProduction6_4";

    @Test
    @Parameters({"verticaJaasConfig", "verticaKrb5Conf", "verticaUrl", "verticaUsername", "verticaPassword",
            "kerberosUser", "keytabPath", "hiveUrl", "hiveUsername", "hivePassword",
            "targetDatabase", "tcProduction6_4InputFile"})
    // the input parameters are different for different use cases
    public void test(String verticaJaasConfig, String verticaKrb5Conf, String verticaUrl, String verticaUsername, String verticaPassword,
                     String kerberosUser, String keytabPath, String hiveUrl, String hiveUsername, String hivePassword,
                     String targetDatabase, String tcProduction6_4InputFile) throws Exception {
        TCRunLog.info(TEST_CASE + " start");
        TCRunLog.info(TEST_CASE + " start reading input file");

        final int MAPPING_DATABASE_INDEX = 0;
        final int MAPPING_TABLE_INDEX = 1;
        final int MAPPING_COLUMNS_INDEX = 3;
        final int HUB_DATABASE_INDEX = 0;
        final int HUB_TABLE_INDEX = 1;
        final int HUB_COLUMNS_INDEX = 3;
        final int DATABASE_INDEX = 0;
        final int TABLE_INDEX = 1;
        final int COLUMN_INDEX = 3;
        final int TABLE_TYPE_INDEX = 9;

        CSVFormat csvFormat = CSVFormat.newFormat(',');
        csvFormat = csvFormat.withSkipHeaderRecord(false);
        csvFormat = csvFormat.withIgnoreEmptyLines(true);
        csvFormat = csvFormat.withQuote('"');

        List<CSVRecord> csvLineValues = new ArrayList<>();
        Map<String, List<String>> mappingTableWithRelatedColumns = new TreeMap<>();
        Map<String, List<String>> hubTableWithRelatedColumns = new TreeMap<>();

        try (FileInputStream fis = new FileInputStream(tcProduction6_4InputFile);
             //file path to input stream
             InputStreamReader isr = new InputStreamReader(fis, "utf-8");
             //input stream to stream reader
             CSVParser csvParser = new CSVParser(isr, csvFormat)
             //stream reader to parser
        ) {
            Consumer<CSVRecord> consumer = record -> {
                TCRunLog.debug(TEST_CASE +
                        " handle line:" + record.getRecordNumber() +
                        ", record:" + record.toString());
                csvLineValues.add(record);

//                String mappingTableName = record.get(MAPPING_TABLE_INDEX);
//                List<String> mappingRelatedColumns = mappingTableWithRelatedColumns.get(mappingTableName);
//                // If there is no such a table, create a new ArrayList, add the column and insert a map
//                if (mappingRelatedColumns == null) {
//                    mappingRelatedColumns = new ArrayList<>();
//                    mappingRelatedColumns.add(record.get(MAPPING_TABLE_INDEX));
//                    mappingTableWithRelatedColumns.put(mappingTableName, mappingRelatedColumns);
//                } else {
//                    mappingRelatedColumns.add(record.get(MAPPING_TABLE_INDEX));
//                }
//                TCRunLog.info("Adding: " + mappingTableName + "." + mappingRelatedColumns);
//
//                String hubTableName = record.get(HUB_TABLE_INDEX);
//                List<String> hubRelatedColumns = hubTableWithRelatedColumns.get(hubTableName);
//                // If there is no such a table, create a new ArrayList, add the column and insert a map
//                if (hubRelatedColumns == null) {
//                    hubRelatedColumns = new ArrayList<>();
//                    hubRelatedColumns.add(record.get(HUB_TABLE_INDEX));
//                    hubTableWithRelatedColumns.put(hubTableName, hubRelatedColumns);
//                } else {
//                    hubRelatedColumns.add(record.get(HUB_TABLE_INDEX));
//                }
//                TCRunLog.info("Adding: " + hubTableName + "." + hubRelatedColumns);
            };
            csvParser.forEach(consumer);
        }
        TCRunLog.info(TEST_CASE + " end read input file");

        HiveConnectionUtil.kerberosLogin(verticaKrb5Conf, kerberosUser, keytabPath);
        try (Connection hiveConnection = HiveConnectionUtil.getConnection(hiveUrl, hiveUsername, hivePassword);
             Statement mappingStatement = hiveConnection.createStatement();
             Connection verticaConnection = VeticaConnectionUtil.getConnectionWithKrb(verticaJaasConfig, verticaKrb5Conf, verticaUrl, verticaUsername, verticaPassword);
             Statement hubStatement = verticaConnection.createStatement()) {
            TCRunLog.info(TEST_CASE + "connect to Hive and Vertica succeeded");

            for (int i = 1; i < csvLineValues.size(); i++) {
                List<String> values = new ArrayList<>();
                CSVRecord csvRecord = csvLineValues.get(i);
                for (int j = 0; j < csvRecord.size(); j++) values.add(csvRecord.get(j));
                TCRunLog.debug("This Line contains: " + values.toString());

                String mappingDatabase = values.get(MAPPING_DATABASE_INDEX);
                String mappingTable = values.get(MAPPING_TABLE_INDEX);
                String mappingColumn = values.get(MAPPING_COLUMNS_INDEX);
                String hubDatabase = values.get(HUB_DATABASE_INDEX);
                String hubTable = values.get(HUB_TABLE_INDEX);
                String hubColumn = values.get(HUB_COLUMNS_INDEX);

                String mappingSQL = "SELECT " + mappingColumn + " FROM " + mappingDatabase + "." + mappingTable;
                TCRunLog.debug("Generated statement: " + mappingSQL);
                ResultSet mappingResultSet = mappingStatement.executeQuery(mappingSQL);
                TCRunLog.debug("Mapping SQL query execution completed");
                List<String> mappingColumnValues = new ArrayList<>();
                while (mappingResultSet.next()) {
                    mappingColumnValues.add(mappingResultSet.getString(1));
                    TCRunLog.debug("Added mapping result: " + mappingResultSet.getString(1));
                }
//                TCRunLog.debug("End writing mapping output file");

                String hubSQL = "SELECT " + hubColumn + " FROM " + hubDatabase + "." + hubTable;
                TCRunLog.debug("Generated statement: " + hubSQL);
                ResultSet hubResultSet = hubStatement.executeQuery(hubSQL);
                TCRunLog.debug("Hub SQL query execution completed");
                List<String> hubColumnValues = new ArrayList<>();
                while (hubResultSet.next()) {
                    hubColumnValues.add(hubResultSet.getString(1));
                    TCRunLog.debug("Added hub result: " + hubResultSet.getString(1));
                }
//                TCRunLog.debug("End writing hub output file");

//                TCResult.info("For " + mappingTable + "." + mappingColumn);
                boolean isTestCasePassed = true;
                for (String value : mappingColumnValues) {
                    if (hubColumnValues.contains(value)) {
                        TCResult.info(hubTable + ";" + hubColumn + ";" + mappingTable + ";" + value + ";True");
                    } else {
                        TCResult.info(hubTable + ";" + hubColumn + ";" + mappingTable + ";" + value + ";False");
                        isTestCasePassed = false;
                    }
                }
//                TCResult.info("============= SUMMARY BELOW =============");
                if (isTestCasePassed) TCResultSummary.info(hubTable + "," + hubColumn + "," + "Passed");
                else TCResultSummary.info(hubTable + "," + hubColumn + "," + "Failed");

                TCRunLog.info("End writing output file");
            }
        }
    }
}
