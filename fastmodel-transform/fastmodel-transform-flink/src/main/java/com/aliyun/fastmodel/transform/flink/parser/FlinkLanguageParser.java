package com.aliyun.fastmodel.transform.flink.parser;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.flink.parser.visitor.FlinkAstBuilder;
import com.aliyun.fastmodel.transform.flink.parser.visitor.FlinkParserAstBuilder;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

/**
 * @author 子梁
 * @date 2024/5/15
 */
@AutoService(LanguageParser.class)
@Slf4j
public class FlinkLanguageParser implements LanguageParser<Node, ReverseContext> {

    /**
     * Flink SQL parse模式：Flink官方parser SDK，Antlr
     */
    public static final String FLINK_PARSE_MODE_KEY ="FLINK_PARSE_MODE";
    public static final String FLINK_PARSE_MODE_SDK ="FLINK_SDK";

    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        if (context == null || CollectionUtils.isEmpty(context.getProperties())) {
            // 默认走antlr
            return parseNodeByAntlr(text, context, FlinkSqlParser::singleStatement);
        }
        List<Property> properties = context.getProperties();
        Optional<Property> parseModeOpt = properties.stream()
            .filter(property -> StringUtils.equalsIgnoreCase(FLINK_PARSE_MODE_KEY, property.getName()))
            .findAny();
        if (parseModeOpt.isPresent() && StringUtils.equalsIgnoreCase(FLINK_PARSE_MODE_SDK, parseModeOpt.get().getValue())) {
            return parseNodeByFlinkParser(text, context);
        }
        return parseNodeByAntlr(text, context, FlinkSqlParser::singleStatement);
    }

    private Node parseNodeByAntlr(String text, ReverseContext context,
                                  Function<com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser, ParserRuleContext> functionalInterface) {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, charStream -> new com.aliyun.fastmodel.transform.flink.parser.FlinkSqlLexer(charStream),
            tokenStream -> new com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser(tokenStream),
            parser -> {
                com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser dorisParser = (com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser)parser;
                return dorisParser.sqlStatements();
            }
        );
        return parserRuleContext.accept(new FlinkAstBuilder(context));
    }

    private Node parseNodeByFlinkParser(String text, ReverseContext context) {
        String formatText = text;
        while (StringUtils.isNotBlank(formatText) && formatText.endsWith(";")) {
            formatText = formatText.substring(0, formatText.length() - 1);
        }

        // 创建SqlParser.Config配置
        SqlParser.Config config = SqlParser.config()
            .withParserFactory(FlinkSqlParserImpl.FACTORY)
            .withQuoting(Quoting.BACK_TICK_BACKSLASH)
            .withQuotedCasing(Casing.UNCHANGED)
            .withUnquotedCasing(Casing.UNCHANGED);

        // 创建SqlParser实例
        SqlParser parser = SqlParser.create(formatText, config);

        try {
            // 解析SQL为AST
            SqlNode sqlNode = parser.parseStmt();
            if (!(sqlNode instanceof SqlCreateTable)) {
                throw new ParseException("Only support create table");
            }

            SqlCreateTable createTable = (SqlCreateTable) sqlNode;
            return createTable.accept(new FlinkParserAstBuilder(context));
        } catch (SqlParseException e) {
            log.error("parse sql error, text = {}", formatText, e);
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    public BaseDataType parseDataType(String text, ReverseContext context) throws ParseException {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, charStream -> new com.aliyun.fastmodel.transform.flink.parser.FlinkSqlLexer(charStream),
            tokenStream -> new com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser(tokenStream),
            parser -> {
                com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser flinkSqlParser = (com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser) parser;
                return flinkSqlParser.columnType();
            }
        );
        return (BaseDataType)parserRuleContext.accept(new FlinkAstBuilder(context));
    }

}
