/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.plantuml;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Plant UML engine
 *
 * @author panguanjing
 * @date 2020/9/18
 */
public class PlantUmlTransformerTest {

    PlantUmlTransformer plantUmlEngine = new PlantUmlTransformer();

    @Test
    public void toPlantUmlTest() throws IOException {
        BufferedReader bufferedReader = new BufferedReader(
            new InputStreamReader(getClass().getResourceAsStream("/plantuml.sql")));

        String t = IOUtils.toString(bufferedReader);
        DomainLanguage domainLanguage = new DomainLanguage(t);
        List<BaseStatement> statements = FastModelParserFactory.getInstance().get().multiParse(domainLanguage);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
        plantUmlEngine.generate(statements, byteArrayOutputStream);
        String output = IOUtils.toString(byteArrayOutputStream.toByteArray());
        assertEquals(output, "@startuml\n"
            + "skinparam dpi 150\n"
            + "\n"
            + "hide methods\n"
            + "hide stereotypes\n"
            + "\n"
            + "''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''\n"
            + "' Functions\n"
            + "''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''\n"
            + "\n"
            + "''\n"
            + "'' 表\n"
            + "''\n"
            + "'' @param string $name 表名\n"
            + "'' @param string $desc 详细描述\n"
            + "''\n"
            + "!unquoted function Table($name, $desc = \"\")\n"
            + "    !return \"class \" + $name + ' as \"' + $name + \"\\n\" + \"(\" + $desc + ')\" << (T,#FFAAAA) >>'\n"
            + "!endfunction\n"
            + "\n"
            + "''\n"
            + "'' 主键\n"
            + "''\n"
            + "'' @param string $col 列名\n"
            + "''\n"
            + "!unquoted function PK($col = \"id\")\n"
            + "    !return \"<b>\" + $col + \"</b>\"\n"
            + "!endfunction\n"
            + "\n"
            + "''\n"
            + "'' 非空\n"
            + "''\n"
            + "'' @param string $col 列名\n"
            + "''\n"
            + "!function NN($col)\n"
            + "    !return \"<u>\" + $col + \"</u>\"\n"
            + "!endfunction\n"
            + "\n"
            + "''\n"
            + "'' 唯一\n"
            + "''\n"
            + "'' @param string $col 列名\n"
            + "''\n"
            + "!function UQ($col)\n"
            + "    !return \"<color:green>\" + $col + \"</color>\"\n"
            + "!endfunction\n"
            + "\n"
            + "''\n"
            + "'' 缺省值\n"
            + "''\n"
            + "'' @param string $val 缺省值\n"
            + "''\n"
            + "!function DV($val)\n"
            + "    !return \"<u>\" + $val + \"</u>\"\n"
            + "!endfunction\n"
            + "\n"
            + "''\n"
            + "'' 无符号数值，unsigned\n"
            + "''\n"
            + "'' @param string $type 类型\n"
            + "''\n"
            + "!function UN($type)\n"
            + "    !return \"U_\" + $type\n"
            + "!endfunction\n"
            + "\n"
            + "''\n"
            + "'' 注释\n"
            + "''\n"
            + "'' @param string $label 列标题\n"
            + "'' @param string $desc 详细描述\n"
            + "''\n"
            + "!function CM($label, $desc=\"\")\n"
            + "    !$val = \"<color:green>\" + $label\n"
            + "    !if ($desc != \"\")\n"
            + "        !$val = $val + \"\\n\" + \"<size:8><color:gray><i>“\" + $desc + \"”</i></color></size>\"\n"
            + "    !endif\n"
            + "    !return $val\n"
            + "!endfunction\n"
            + "\n"
            + "''\n"
            + "'' 列\n"
            + "''\n"
            + "'' @param string $name 列名\n"
            + "'' @param string $type 数据类型\n"
            + "'' @param boolean $notNull 是否非空 0:可空，1:非空，缺省为0\n"
            + "'' @param mixed $defVal 缺省值 无缺省值时传空字符串\"\"，缺省值为空字符串时传单引号\"''\"\n"
            + "'' @param string $label 列标题\n"
            + "'' @param string $desc 详细描述\n"
            + "'' \n"
            + "!function Column($name, $type, $notNull=0, $defVal=\"\", $label=\"\", $desc=\"\")\n"
            + "    !$val = \"\"\n"
            + "    !if ($notNull == 1)\n"
            + "        !$name = NN($name)\n"
            + "    !endif\n"
            + "    !$val = $name + \" <color:royalBlue><size:8>\" + %upper($type)\n"
            + "    !if ($defVal != \"\")\n"
            + "        !$val = $val + \" \" + DV($defVal)\n"
            + "    !endif\n"
            + "    !if ($label != \"\")\n"
            + "        !$val = $val + \" \" + CM($label, $desc)\n"
            + "    !endif\n"
            + "    !return $val\n"
            + "!endfunction\n"
            + "\n"
            + "''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''\n"
            + "' Constants\n"
            + "''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''\n"
            + "\n"
            + "' created_at、updated_at、deleted_at三列\n"
            + "!global TIMESTAMPS = Column(\"created_at\", \"TIMESTAMP\") + \"\\n\" + Column(\"updated_at\", "
            + "\"TIMESTAMP\") + \"\\n\" + Column(\"deleted_at\", \"TIMESTAMP\")\n"
            + "\n"
            + "' 主键类型\n"
            + "!global PK_TYPE = UN(\"BIGINT[20]\")\n"
            + "\n"
            + "' 主键列\n"
            + "!$tmp = PK()\n"
            + "!global PRIMARY_KEY = Column($tmp, PK_TYPE)\n"
            + "\n"
            + "' 列注释中的换行符\n"
            + "!global CMBR=\"\\n<size:8><color:gray><i>\"\n"
            + "Table(dim_org,企业维度表) {\n"
            + "Column(PK(\"org_id\"), \"BIGINT\", 1,\"\", \"\", \"企业Id\")\n"
            + "Column(\"org_name\", \"STRING\", 0,\"\", \"\", \"企业名称\")\n"
            + "Column(\"list_data\", \"ARRAY<BIGINT>\", 0,\"\", \"\", \"\")\n"
            + "}\n"
            + "\n"
            + "Table(fact_t1,人员变更事实表) {\n"
            + "Column(\"org_id\", \"BIGINT\", 0,\"\", \"\", \"企业Id\")\n"
            + "}\n"
            + "fact_t1 --> dim_org\n"
            + "\n"
            + "@enduml");
    }

    @Test
    public void testTransform() {
        List<BaseStatement> list = new ArrayList<>();
        List<ColumnDefinition> columnDefines = getColNameType();
        CreateTable createTableStatement = CreateFactTable.builder().tableName(QualifiedName.of("t1")).columns(
            columnDefines
        ).comment(new Comment("comment")).build();
        list.add(createTableStatement);
        CompositeStatement compositeStatement = new CompositeStatement(list);
        DialectNode transform = plantUmlEngine.transform(compositeStatement);
        assertTrue(transform.getNode().contains("Table"));
    }

    private List<ColumnDefinition> getColNameType() {
        List<ColumnDefinition> list = new ArrayList<>();
        ColumnDefinition columnDefine = ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build();
        list.add(columnDefine);
        return list;
    }

}
