package com.aliyun.fastmodel.transform.oceanbase.client;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * ddl generator test
 *
 * @author panguanjing
 * @date 2023/9/17
 */
public class OceanBaseGeneratorAlterTest {
    CodeGenerator codeGenerator = new DefaultCodeGenerator();

    @Test
    public void testGeneratorAddColumn() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> oneColumns = Lists.newArrayList();
        Column e1 = Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(true)
            .build();
        oneColumns.add(e1);

        List<Column> twoColumns = Lists.newArrayList();
        twoColumns.add(e1);
        twoColumns.add(Column.builder()
            .dataType("int")
            .name("c2")
            .comment("comment")
            .build());
        twoColumns.add(Column.builder()
            .dataType("int")
            .name("c3")
            .comment("comment")
            .build());
        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(twoColumns)
            .comment("comment")
            .build();
        request.setAfter(after);
        Table before = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(oneColumns)
            .comment("comment2")
            .build();
        request.setBefore(before);
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.OB_MYSQL.getValue(), IVersion.DEFAULT_VERSION))
            .build());
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE autotest.abc\n"
                + "ADD COLUMN c2 INT NOT NULL COMMENT 'comment',\n"
                + "ADD COLUMN c3 INT NOT NULL COMMENT 'comment';\n"
                + "ALTER TABLE autotest.abc SET COMMENT 'comment'",
            dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(";\n")));
    }

    @Test
    public void testGeneratorDropColumn() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> beforeColumns = Lists.newArrayList();

        beforeColumns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(false)
            .build());

        Column build = Column.builder()
            .dataType("int")
            .name("c2")
            .partitionKeyIndex(0)
            .partitionKey(true)
            .comment("comment")
            .build();
        beforeColumns.add(build);

        List<Column> afterColumns = Lists.newArrayList();
        afterColumns.add(build);

        Table before = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .comment("comment1")
            .build();

        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(afterColumns)
            .build();

        request.setBefore(before);
        request.setAfter(after);
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.OB_MYSQL.getValue(), IVersion.DEFAULT_VERSION))
            .build());
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE autotest.abc DROP COLUMN c1,\n"
            + "ALTER TABLE autotest.abc SET COMMENT ''", dialectNodes.stream().filter(DialectNode::isExecutable).map(DialectNode::getNode)
            .collect(Collectors.joining(",\n")));
    }

    @Test
    public void testGeneratorModifyColumn() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> beforeColumns = Lists.newArrayList();

        beforeColumns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(false)
            .build());

        Column build = Column.builder()
            .dataType("int")
            .name("c1")
            .comment("comment")
            .nullable(true)
            .build();

        List<Column> afterColumns = Lists.newArrayList();
        afterColumns.add(build);

        Table before = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .build();

        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(afterColumns)
            .build();

        request.setBefore(before);
        request.setAfter(after);
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.OB_MYSQL.getValue(), IVersion.DEFAULT_VERSION))
            .build());
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE autotest.abc CHANGE COLUMN c1 c1 INT COMMENT 'comment'", dialectNodes.stream().filter(DialectNode::isExecutable)
            .map(DialectNode::getNode)
            .collect(Collectors.joining(",\n")));
    }

    @Test
    public void testGeneratorSetProperties() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> beforeColumns = Lists.newArrayList();

        beforeColumns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(false)
            .build());

        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey("default.replication_num");
        stringProperty.setValueString("2");
        List<BaseClientProperty> properties = Lists.newArrayList(
            stringProperty
        );
        Table before = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .build();

        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .properties(properties)
            .build();

        request.setBefore(before);
        request.setAfter(after);
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.OB_MYSQL.getValue(), IVersion.DEFAULT_VERSION))
            .build());
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE autotest.abc default.replication_num='2'\n", dialectNodes.stream().filter(DialectNode::isExecutable)
            .map(DialectNode::getNode)
            .collect(Collectors.joining(",\n")));

    }

    @Test
    public void testGeneratorUnSetProperties() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> beforeColumns = Lists.newArrayList();

        beforeColumns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(false)
            .build());

        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey("default.replication_num");
        stringProperty.setValueString("2");
        List<BaseClientProperty> properties = Lists.newArrayList(
            stringProperty
        );
        Table before = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .properties(properties)
            .build();

        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .build();

        request.setBefore(before);
        request.setAfter(after);
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.OB_MYSQL.getValue(), IVersion.DEFAULT_VERSION))
            .build());
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE autotest.abc default.replication_num=''", dialectNodes.stream().filter(DialectNode::isExecutable)
            .map(DialectNode::getNode)
            .collect(Collectors.joining(",\n")));

    }
}
