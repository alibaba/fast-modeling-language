package com.aliyun.fastmodel.transform.sqlite.parser;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/8/28
 */
@Slf4j
public class SqliteLanguageParserTest {

    SqliteLanguageParser sqliteLanguageParser = new SqliteLanguageParser();

    @Test
    public void parseNode() {
        CreateTable createTable = sqliteLanguageParser.parseNode(
            "CREATE TABLE overall_team_standings (overall_team_standings_id AUTO_INCREMENT PRIMARY KEY, Rank INT, Team "
                + "VARCHAR, Round1 INT, Round2 INT, Round3 INT, Round4 INT, Round5 INT, Total Points INT)");
        assertEquals("overall_team_standings", createTable.getQualifiedName().toString());
        assertEquals(9, createTable.getColumnDefines().size());
        assertEquals("CREATE DIM TABLE overall_team_standings \n"
            + "(\n"
            + "   overall_team_standings_id AUTO_INCREMENT PRIMARY KEY,\n"
            + "   Rank                      INT,\n"
            + "   Team                      VARCHAR,\n"
            + "   Round1                    INT,\n"
            + "   Round2                    INT,\n"
            + "   Round3                    INT,\n"
            + "   Round4                    INT,\n"
            + "   Round5                    INT,\n"
            + "   Total                     Points\n"
            + ")", createTable.toString());
    }

    @Data
    static class SpiderData {
        private String instruction;
        private String output;
    }

    @Test
    public void testReverse() {
        CreateTable createTable = sqliteLanguageParser.parseNode("CREATE TABLE \"lists\"\n"
            + "(\n"
            + "user_id INTEGER\n"
            + "references lists_users (user_id),\n"
            + "list_id INTEGER not null\n"
            + "key_5 key COMMENT 'primary',\n"
            + "title TEXT COMMENT 'list title',\n"
            + "list_movie_number INTEGER,\n"
            + "key_8 TEXT COMMENT 'list update timestamp utc',\n"
            + "key_9 TEXT COMMENT 'list creation timestamp utc',\n"
            + "llowers INTEGER COMMENT 'list followers',\n"
            + "url TEXT COMMENT 'list url',\n"
            + "list_comments INTEGER,\n"
            + "key_13 TEXT COMMENT 'list description',\n"
            + "list_cover TEXT COMMENT 'list cover image url',\n"
            + "list_first_image_url TEXT,\n"
            + "key_16 TEXT COMMENT 'list second image url',\n"
            + "list_third_image_url TEXT\n"
            + ")");
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        assertEquals(14, columnDefines.size());
        String collect = columnDefines.stream().map(c -> {
            return c.getDataType().toString();
        }).collect(Collectors.joining(","));
        assertEquals("INTEGER,INTEGER,TEXT,INTEGER,TEXT,TEXT,INTEGER,TEXT,INTEGER,TEXT,TEXT,TEXT,TEXT,TEXT", collect);
    }

    @Test
    public void testReverseBatch() {
        CompositeStatement createTable = sqliteLanguageParser.parseNode(
            "create table a (a text)\n\ncreate table b (a integer)"
        );
        assertEquals(2, createTable.getStatements().size());
    }

    @Test
    public void testReverseForeignKey() {
        CreateTable createTable = sqliteLanguageParser.parseNode(
            "CREATE TABLE `countries` (`COUNTRY_ID` varchar(2) NOT NULL,`COUNTRY_NAME` varchar(40) DEFAULT NULL,"
                + "`REGION_ID` decimal(10,0) DEFAULT NULL,PRIMARY KEY (`COUNTRY_ID`),FOREIGN KEY (`REGION_ID`) REFERENCES regions (`REGION_ID`), "
                + "FOREIGN KEY (`COUNTRY_NAME`) REFERENCES regions (`COUNTRY_NAME`))");
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        assertEquals(3, constraintStatements.size());
        BaseConstraint baseConstraint = constraintStatements.get(0);
        assertTrue(baseConstraint instanceof PrimaryConstraint);
        baseConstraint = constraintStatements.get(1);
        assertTrue(baseConstraint instanceof DimConstraint);
        DimConstraint constraint = (DimConstraint)baseConstraint;
        assertEquals("REGION_ID", constraint.getColNames().stream().map(Identifier::getValue).collect(Collectors.joining()));
    }

    @Test
    public void testReverseForeignKeyWithoutColumns() {
        CreateTable createTable = sqliteLanguageParser.parseNode(
            "CREATE TABLE `countries` (`COUNTRY_ID` varchar(2) NOT NULL,`COUNTRY_NAME` varchar(40) DEFAULT NULL,"
                + "`REGION_ID` decimal(10,0) DEFAULT NULL,PRIMARY KEY (`COUNTRY_ID`),FOREIGN KEY (`REGION_ID`) REFERENCES regions, "
                + "FOREIGN KEY (`COUNTRY_NAME`) REFERENCES regions (`COUNTRY_NAME`))");

        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        assertEquals(3, constraintStatements.size());
        DimConstraint dimConstraint = (DimConstraint)constraintStatements.get(1);
        List<Identifier> referenceColNames = dimConstraint.getReferenceColNames();
        assertEquals(1, referenceColNames.size());
    }
}