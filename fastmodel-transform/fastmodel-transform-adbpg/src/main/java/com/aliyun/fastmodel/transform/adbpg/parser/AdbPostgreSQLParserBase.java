/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.adbpg.parser;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.TokenStream;

public abstract class AdbPostgreSQLParserBase extends Parser {
    public AdbPostgreSQLParserBase self;

    public List<AdbPostgreSQLParseError> ParseErrors = new ArrayList<AdbPostgreSQLParseError>();

    public AdbPostgreSQLParserBase(TokenStream input) {
        super(input);
        self = this;
    }

    ParserRuleContext GetParsedSqlTree(String script, int line) {
        AdbPostgreSQLParser ph = getPostgreSQLParser(script);
        ParserRuleContext result = ph.root();
        for (AdbPostgreSQLParseError err : ph.ParseErrors) {
            ParseErrors.add(new AdbPostgreSQLParseError(err.getNumber(), err.getOffset(), err.getLine() + line, err.getColumn(), err.getMessage()));
        }
        return result;
    }

    public void ParseRoutineBody(AdbPostgreSQLParser.Createfunc_opt_listContext _localctx) {
        String lang = null;
        for (AdbPostgreSQLParser.Createfunc_opt_itemContext coi : _localctx.createfunc_opt_item()) {
            if (coi.LANGUAGE() != null) {
                if (coi.nonreservedword_or_sconst() != null) {
                    if (coi.nonreservedword_or_sconst().nonreservedword() != null) {
                        if (coi.nonreservedword_or_sconst().nonreservedword().identifier() != null) {
                            if (coi.nonreservedword_or_sconst().nonreservedword().identifier()
                                .Identifier() != null) {
                                lang = coi.nonreservedword_or_sconst().nonreservedword().identifier()
                                    .Identifier().getText();
                                break;
                            }
                        }
                    }
                }
            }
        }
        if (null == lang) {return;}
        AdbPostgreSQLParser.Createfunc_opt_itemContext func_as = null;
        for (AdbPostgreSQLParser.Createfunc_opt_itemContext a : _localctx.createfunc_opt_item()) {
            if (a.func_as() != null) {
                func_as = a;
                break;

            }

        }
        if (func_as != null) {
            String txt = GetRoutineBodyString(func_as.func_as().sconst(0));
            int line = func_as.func_as().sconst(0).start.getLine();
            AdbPostgreSQLParser ph = getPostgreSQLParser(txt);
            switch (lang) {
                case "plpgsql":
                    func_as.func_as().Definition = ph.plsqlroot();
                    break;
                case "sql":
                    func_as.func_as().Definition = ph.root();
                    break;
            }
            for (AdbPostgreSQLParseError err : ph.ParseErrors) {
                ParseErrors.add(
                    new AdbPostgreSQLParseError(err.getNumber(), err.getOffset(), err.getLine() + line, err.getColumn(), err.getMessage()));
            }
        }

    }

    private static String TrimQuotes(String s) {
        return (s == null || s.isEmpty()) ? s : s.substring(1, s.length() - 2);
    }

    public static String unquote(String s) {
        int slength = s.length();
        StringBuilder r = new StringBuilder(slength);
        int i = 0;
        while (i < slength) {
            Character c = s.charAt(i);
            r.append(c);
            if (c == '\'' && i < slength - 1 && (s.charAt(i + 1) == '\'')) {i++;}
            i++;
        }
        return r.toString();
    }

    public static String GetRoutineBodyString(AdbPostgreSQLParser.SconstContext rule) {
        AdbPostgreSQLParser.AnysconstContext anysconst = rule.anysconst();
        org.antlr.v4.runtime.tree.TerminalNode StringConstant = anysconst.StringConstant();
        if (null != StringConstant) {return unquote(TrimQuotes(StringConstant.getText()));}
        org.antlr.v4.runtime.tree.TerminalNode UnicodeEscapeStringConstant = anysconst.UnicodeEscapeStringConstant();
        if (null != UnicodeEscapeStringConstant) {return TrimQuotes(UnicodeEscapeStringConstant.getText());}
        org.antlr.v4.runtime.tree.TerminalNode EscapeStringConstant = anysconst.EscapeStringConstant();
        if (null != EscapeStringConstant) {return TrimQuotes(EscapeStringConstant.getText());}
        String result = "";
        List<org.antlr.v4.runtime.tree.TerminalNode> dollartext = anysconst.DollarText();
        for (org.antlr.v4.runtime.tree.TerminalNode s : dollartext) {
            result += s.getText();
        }
        return result;
    }

    public static AdbPostgreSQLParser getPostgreSQLParser(String script) {
        CharStream charStream = CharStreams.fromString(script);
        Lexer lexer = new AdbPostgreSQLLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        AdbPostgreSQLParser parser = new AdbPostgreSQLParser(tokens);
        AdbPostgreSQLParserErrorListener errorListener = new AdbPostgreSQLParserErrorListener();
        errorListener.grammar = parser;
        parser.addErrorListener(errorListener);
        return parser;
    }
}
