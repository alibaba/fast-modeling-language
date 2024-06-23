package com.aliyun.fastmodel.transform.oceanbase.client.converter;

import java.util.List;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseHashPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBasePartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.BaseSubPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubHashPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.HashPartitionElement;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * OceanBaseMysqlClientConverterTest
 *
 * @author panguanjing
 * @date 2024/2/5
 */
public class OceanBaseMysqlClientConverterTest {

    OceanBaseMysqlClientConverter oceanBaseMysqlClientConverter = new OceanBaseMysqlClientConverter();

    @Test
    public void getLanguageParser() {
        LanguageParser languageParser = oceanBaseMysqlClientConverter.getLanguageParser();
        assertNotNull(languageParser);
    }

    @Test
    public void testGetRaw() {
        LongLiteral count = new LongLiteral("1");
        BaseExpression expression = new StringLiteral("1");
        BaseExpression e = new StringLiteral("c");
        BaseSubPartition subPartition = new SubHashPartition(
            e, null
        );
        List<HashPartitionElement> hashPartitionElements = Lists.newArrayList();
        HashPartitionElement hashPartitionElement = new HashPartitionElement(
            QualifiedName.of("c1"),
            new LongLiteral("1"),
            null,
            null
        );
        OceanBasePartitionBy partition = new OceanBaseHashPartitionBy(count, expression, subPartition, hashPartitionElements);
        String raw = oceanBaseMysqlClientConverter.getRaw(partition);
        assertEquals("PARTITION BY HASH ('1') SUBPARTITION BY HASH ('c') PARTITIONS 1", raw);
    }

    @Test
    public void testConverter() {
        PropertyConverter propertyConverter = oceanBaseMysqlClientConverter.getPropertyConverter();
        BaseClientProperty baseClientProperty = propertyConverter.create("a", "b");
        assertNotNull(baseClientProperty);
    }
}