package com.aliyun.fastmodel.transform.doris.client;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ListPartitionValue;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionValue;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * DorisClientConverterTest
 *
 * @author panguanjing
 * @date 2024/2/21
 */
public class DorisClientConverterTest {

    DorisClientConverter dorisClientConverter = new DorisClientConverter();

    @Test
    public void getDataTypeName() {
        IDataTypeName bigint = dorisClientConverter.getDataTypeName("bigint");
        assertNotNull(bigint);
    }

    @Test
    public void getLanguageParser() {
        LanguageParser languageParser = dorisClientConverter.getLanguageParser();
        assertNotNull(languageParser);
    }

    @Test
    public void getRaw() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("c1")).dataType(DataTypeUtil.simpleType("bigint", null)).build());
        ArrayList<PartitionDesc> rangePartitions = Lists.newArrayList();
        SingleRangePartition singleRangePartition = new SingleRangePartition(
            new Identifier("a"),
            false,
            null, null
        );
        rangePartitions.add(singleRangePartition);
        PartitionedBy partitionedBy = new RangePartitionedBy(columns, rangePartitions);
        String raw = dorisClientConverter.getRaw(partitionedBy);
        assertEquals("PARTITION BY RANGE (c1)\n"
            + "(\n"
            + "   PARTITION a VALUES \n"
            + ")", raw);
    }

    @Test
    public void getRawListPartitionValue() {
        BaseExpression stringLiteral = new StringLiteral("abc");
        List<PartitionValue> partitonValueList = Lists.newArrayList(
            new PartitionValue(false, stringLiteral)
        );
        ListPartitionValue listPartitionValue = new ListPartitionValue(
            partitonValueList
        );
        String raw = dorisClientConverter.getRaw(listPartitionValue);
        assertEquals("\"abc\"", raw);
    }

    @Test
    public void getPropertyConverter() {
        PropertyConverter propertyConverter = dorisClientConverter.getPropertyConverter();
        assertNotNull(propertyConverter);
    }
}