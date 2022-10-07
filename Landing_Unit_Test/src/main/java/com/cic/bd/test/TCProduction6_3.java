package com.cic.bd.test;

import com.cic.bd.test.util.VeticaConnectionUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;

public class TCProduction6_3 {
    private static Logger TCRunLog = Logger.getLogger(TCProduction6_3.class);
    private static Logger TCResult = Logger.getLogger("TCProduction6_3");
    private static final String TEST_CASE = "TCProduction6_3";

    private List<String> statements = new ArrayList<>();


    @Test
    @Parameters({"verticaJaasConfig", "verticaKrb5Conf", "verticaUrl", "verticaUsername", "verticaPassword",
            "targetDatabase", "tcProduction6_3InputFile"})
    // the input parameters are different for different use cases
    public void test(String verticaJaasConfig, String verticaKrb5Conf, String verticaUrl, String verticaUsername, String verticaPassword,
                     String targetDatabase, String tcProduction6_3InputFile) throws Exception {
        TCRunLog.info(TEST_CASE + " start");
        TCRunLog.info(TEST_CASE + " start reading input file");

        final int MAPPING_DATABASE_INDEX = 0;
        final int MAPPING_TABLE_INDEX = 1;
        final int MAPPING_COLUMNS_INDEX = 2;
        final int MAPPING_COLUMN_TYPE_INDEX = 3;
        final int SOURCE_DATABASE_INDEX = 4;
        final int SOURCE_TABLE_INDEX = 5;
        final int SOURCE_COLUMNS_INDEX = 6;
        final int HUB_TABLE_INDEX = 7;
        final int HUB_UUID_COLUMN_INDEX = 8;

        final String template = "SELECT %s%s%s%s%s " +
                "FROM %s.%s src " +
                "LEFT JOIN %s.%s mapping " +
                "ON %s " +
                "LEFT JOIN %s.%s hub " +
                "ON %s " +
                "WHERE %s";

        // Define the replacement units used in the statements
        String mappingDatabase = "";
        String mappingTable = "";
        StringBuilder mappingDLColumns = new StringBuilder();
        StringBuilder mappingNotDLColumns = new StringBuilder();
        String sourceDatabase = "";
        String sourceTable = "";
        StringBuilder sourceColumns = new StringBuilder();
        String hubDatabase = "";
        String hubTable = "";
        String hubUUIDColumn = "";
        StringBuilder hubDLColumns = new StringBuilder();

        StringBuilder onConditionForMapping = new StringBuilder();
        StringBuilder onConditionForHub = new StringBuilder();
        StringBuilder whereCondition = new StringBuilder();

        CSVFormat csvFormat = CSVFormat.newFormat(',');
        csvFormat = csvFormat.withSkipHeaderRecord(false);
        csvFormat = csvFormat.withIgnoreEmptyLines(true);
        csvFormat = csvFormat.withQuote('"');
        List<CSVRecord> csvLineValues = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(tcProduction6_3InputFile);
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
            };
            csvParser.forEach(consumer);
        }
        TCRunLog.info(TEST_CASE + " end read input file");

        try (Connection conn = VeticaConnectionUtil.getConnectionWithKrb(verticaJaasConfig, verticaKrb5Conf, verticaUrl, verticaUsername, verticaPassword);
             Statement tableStmt = conn.createStatement()) {
            TCRunLog.debug(TEST_CASE + "connect to Vertica succeeded and end read tables");

            for (int i = 1; i < csvLineValues.size(); i++) {
                List<String> values = new ArrayList<>();
                CSVRecord csvRecord = csvLineValues.get(i);
                for (int j = 0; j < csvRecord.size(); j++) values.add(csvRecord.get(j));
                // TODO:DEBUG
                TCRunLog.debug("This Line contains: " + values.toString());

                // For each new mapping table
                if (Objects.equals(values.get(SOURCE_DATABASE_INDEX), "")) {
                    // TODO:DEBUG
//                    TCRunLog.debug("Entered 1st IF CONDITION");
                    // Each null value of source database means a finished statement
                    if (sourceColumns.length() != 0) {
                        String statement = constructStatement(template,
                                hubUUIDColumn, hubDLColumns,
                                mappingDLColumns, mappingNotDLColumns, sourceColumns,
                                sourceDatabase, sourceTable,
                                mappingDatabase, mappingTable,
                                onConditionForMapping,
                                hubDatabase, hubTable,
                                onConditionForHub,
                                whereCondition);
                        //TODO:DEBUG
                        TCRunLog.debug("Generate statement: " + statement);
                        ResultSet tableResultSet = tableStmt.executeQuery(statement);
                        TCRunLog.debug("SQL query execution completed");
                        while (tableResultSet.next()) {
                            TCResult.info(tableResultSet.getString(1) + "," + tableResultSet.getString(2));
                        }
                        TCRunLog.debug("End writing output file");
                        TCRunLog.info("This Query Ends");

                        // After construction, set null to the strings
                        hubDLColumns.setLength(0);
                        mappingDLColumns.setLength(0);
                        mappingNotDLColumns.setLength(0);
                        sourceColumns.setLength(0);
                        onConditionForMapping.setLength(0);
                        onConditionForHub.setLength(0);
                        whereCondition.setLength(0);
                    }

                    mappingDLColumns.append("mapping.").append(values.get(MAPPING_COLUMNS_INDEX)).append(", ");
                    hubDLColumns.append("hub.").append(values.get(MAPPING_COLUMNS_INDEX)).append(", ");
                    onConditionForHub.append("mapping.").append(values.get(MAPPING_COLUMNS_INDEX)).append(" = ").append("hub.").append(values.get(MAPPING_COLUMNS_INDEX)).append(" AND ");

                } else if ((!Objects.equals(values.get(SOURCE_DATABASE_INDEX), ""))) {
                    // TODO:DEBUG
//                    TCRunLog.debug("Entered 2nd IF CONDITION");

                    if (!Objects.equals(sourceTable, values.get(SOURCE_TABLE_INDEX)) && !Objects.equals(sourceTable, "")) {
                        String statement = constructStatement(template,
                                hubUUIDColumn, hubDLColumns,
                                mappingDLColumns, mappingNotDLColumns, sourceColumns,
                                sourceDatabase, sourceTable,
                                mappingDatabase, mappingTable,
                                onConditionForMapping,
                                hubDatabase, hubTable,
                                onConditionForHub,
                                whereCondition);
                        //TODO:DEBUG
                        TCRunLog.debug("Generate statement: " + statement);
                        ResultSet tableResultSet = tableStmt.executeQuery(statement);
                        TCRunLog.debug("SQL query execution completed");
                        while (tableResultSet.next()) {
                            TCResult.info(tableResultSet.getString(1) + "," + tableResultSet.getString(2));
                        }
                        TCRunLog.debug("End writing output file");
                        TCRunLog.info("This Query Ends");

                        //hubDLColumns.setLength(0);
                        //mappingDLColumns.setLength(0);
                        mappingNotDLColumns.setLength(0);
                        sourceColumns.setLength(0);
                        onConditionForMapping.setLength(0);
                        //onConditionForHub.setLength(0);
                        whereCondition.setLength(0);
                    }

                    mappingNotDLColumns.append("mapping.").append(values.get(MAPPING_COLUMNS_INDEX)).append(", ");
                    sourceColumns.append("src.").append(values.get(SOURCE_COLUMNS_INDEX)).append(", ");
                    onConditionForMapping.append("src.").append(values.get(SOURCE_COLUMNS_INDEX)).append(" = ").append("mapping.").append(values.get(MAPPING_COLUMNS_INDEX)).append(" AND ");
                    if (Objects.equals(values.get(MAPPING_COLUMN_TYPE_INDEX), "string"))
                        whereCondition.append("mapping.").append(values.get(MAPPING_COLUMNS_INDEX)).append(" <> \"\"").append(" OR ");

                    if (i == csvLineValues.size() - 1) {
                        String statement = constructStatement(template,
                                hubUUIDColumn, hubDLColumns,
                                mappingDLColumns, mappingNotDLColumns, sourceColumns,
                                sourceDatabase, sourceTable,
                                mappingDatabase, mappingTable,
                                onConditionForMapping,
                                hubDatabase, hubTable,
                                onConditionForHub,
                                whereCondition);
                        //TODO:DEBUG
                        TCRunLog.debug("Generated statement: " + statement);
                        ResultSet tableResultSet = tableStmt.executeQuery(statement);
                        TCRunLog.debug("SQL query execution completed");
                        while (tableResultSet.next()) {
                            TCResult.info(tableResultSet.getString(1) + ","
                                    + tableResultSet.getString(2) + ","
                                    + tableResultSet.getString(3) + ","
                                    + tableResultSet.getString(4) + ","
                                    + tableResultSet.getString(5) + ","
                                    + tableResultSet.getString(6) + ",");
                        }
                        TCRunLog.debug("End writing output file");
                        TCRunLog.info("This Query Ends");
                    }
                }

                mappingDatabase = values.get(MAPPING_DATABASE_INDEX);
                mappingTable = values.get(MAPPING_TABLE_INDEX);
                sourceDatabase = values.get(SOURCE_DATABASE_INDEX);
                sourceTable = values.get(SOURCE_TABLE_INDEX);
                hubDatabase = values.get(MAPPING_DATABASE_INDEX);
                hubTable = values.get(HUB_TABLE_INDEX);
                hubUUIDColumn = "hub." + values.get(HUB_UUID_COLUMN_INDEX) + ", ";
            }

            // TODO:DEBUG
//            TCRunLog.debug(statements.toString());
//            TCResult.debug(statements.toString());
        }

        TCRunLog.info(TEST_CASE + " end");
    }

    private String constructStatement(String template,
                                      String hubUUIDColumn, StringBuilder hubDLColumns,
                                      StringBuilder mappingDLColumns, StringBuilder mappingNotDLColumns, StringBuilder sourceColumns,
                                      String sourceDatabase, String sourceTable,
                                      String mappingDatabase, String mappingTable,
                                      StringBuilder onConditionForMapping,
                                      String hubDatabase, String hubTable,
                                      StringBuilder onConditionForHub,
                                      StringBuilder whereCondition) {
        sourceColumns.setLength(sourceColumns.length() - 2);
        onConditionForMapping.setLength(onConditionForMapping.length() - 5);
        onConditionForHub.setLength(onConditionForHub.length() - 5);
        if (whereCondition.length() != 0) whereCondition.setLength(whereCondition.length() - 4);
        // TODO:DEBUG
//        TCRunLog.debug("Entered constructStatement()");
        // Construct the SQL command
        String statement = String.format(template,
                hubUUIDColumn, hubDLColumns,
                mappingDLColumns, mappingNotDLColumns, sourceColumns,
                sourceDatabase, sourceTable,
                mappingDatabase, mappingTable,
                onConditionForMapping,
                hubDatabase, hubTable,
                onConditionForHub,
                whereCondition);
        statements.add(statement);
        return statement;
    }
}
