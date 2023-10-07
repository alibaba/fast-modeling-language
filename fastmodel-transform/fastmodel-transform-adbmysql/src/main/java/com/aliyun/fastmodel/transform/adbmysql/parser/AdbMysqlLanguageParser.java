package com.aliyun.fastmodel.transform.adbmysql.parser;

import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.ThrowingErrorListener;
import com.aliyun.fastmodel.common.parser.lexer.CaseChangingCharStream;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;

/**
 * AdbMysqlLanguageParser
 *
 * @author panguanjing
 * @date 2023/2/10
 */
@AutoService(LanguageParser.class)
public class AdbMysqlLanguageParser implements LanguageParser<Node, ReverseContext> {

    public static final ThrowingErrorListener LISTENER = new ThrowingErrorListener();

    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        return getNode(text, context, AdbMysqlParser::root);
    }

    private Node getNode(String text, ReverseContext context, Function<AdbMysqlParser, ParserRuleContext> functionalInterface) {
        String code = StripUtils.appendSemicolon(text);
        CodePointCharStream charStream = CharStreams.fromString(code);
        CaseChangingCharStream caseChangingCharStream = new CaseChangingCharStream(charStream, true);
        AdbMysqlLexer lexer = new AdbMysqlLexer(caseChangingCharStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(LISTENER);
        CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
        AdbMysqlParser hiveParser = new AdbMysqlParser(commonTokenStream);
        hiveParser.removeErrorListeners();
        hiveParser.addErrorListener(LISTENER);
        ParserRuleContext tree;
        try {
            hiveParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = functionalInterface.apply(hiveParser);
        } catch (Throwable e) {
            commonTokenStream.seek(0);
            hiveParser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = functionalInterface.apply(hiveParser);
        }
        return tree.accept(new AdbMysqlAstBuilder(context));
    }
}


