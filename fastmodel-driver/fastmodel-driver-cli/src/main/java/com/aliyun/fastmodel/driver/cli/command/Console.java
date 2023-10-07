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

package com.aliyun.fastmodel.driver.cli.command;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import com.aliyun.fastmodel.driver.cli.FastModel.VersionProvider;
import com.aliyun.fastmodel.driver.cli.terminal.Completion;
import com.aliyun.fastmodel.driver.cli.terminal.InputHighlighter;
import com.aliyun.fastmodel.driver.cli.terminal.InputParser;
import com.aliyun.fastmodel.driver.cli.terminal.QueryRunner;
import com.aliyun.fastmodel.driver.cli.terminal.TerminalUtils;
import com.aliyun.fastmodel.driver.cli.terminal.printer.AlignedTablePrinter;
import com.aliyun.fastmodel.driver.cli.terminal.printer.OutputPrinter;
import com.aliyun.fastmodel.driver.model.QueryResult;
import com.aliyun.fastmodel.parser.StatementSplitter;
import com.aliyun.fastmodel.parser.StatementSplitter.StatementText;
import com.google.common.base.CharMatcher;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReader.Option;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import static com.google.common.base.CharMatcher.whitespace;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.jline.reader.LineReader.HISTORY_FILE;
import static org.jline.reader.LineReader.SECONDARY_PROMPT_PATTERN;
import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.DEFAULT;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/22
 */
@Command(
    name = "fastmodel",
    header = "Fastmodel command line interface",
    synopsisHeading = "%nUSAGE:%n%n",
    optionListHeading = "%nOPTIONS:%n",
    usageHelpAutoWidth = true,
    versionProvider = VersionProvider.class
)
@Slf4j
public class Console implements Callable<Integer> {

    @Mixin
    public ClientOptions clientOptions;

    private static final String PROMPT = "fastmodel";

    public static final Set<String> STATEMENT_DELIMITERS = ImmutableSet.of(";", "\\G");

    @Override
    public Integer call() throws Exception {
        return run() ? 0 : 1;
    }

    public boolean run() throws Exception {
        AtomicBoolean atomicBoolean = new AtomicBoolean();
        Properties properties = clientOptions.asProperties();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //release system resources
            TerminalUtils.closeTerminal();
        }));
        QueryRunner queryRunner = new QueryRunner(properties);
        Connection connection = queryRunner.getConnection();
        if (connection == null) {
            throw new RuntimeException("Get connection error:" + properties);
        }
        runConsole(queryRunner, atomicBoolean);
        return true;
    }

    private void runConsole(QueryRunner queryRunner, AtomicBoolean exiting) {
        LineReader reader = null;
        try {
            reader = LineReaderBuilder.builder().terminal(TerminalUtils.getTerminal()).
                variable(HISTORY_FILE, getHistoryFile()).
                variable(SECONDARY_PROMPT_PATTERN, colored("%P -> "))
                .variable(
                    LineReader.BLINK_MATCHING_PAREN, 0).parser(new InputParser())
                .highlighter(
                    new InputHighlighter()
                ).completer(new AggregateCompleter(getCompleters())).build();
            reader.unsetOpt(Option.HISTORY_TIMESTAMPED);
            String remaining = "";
            while (!exiting.get()) {
                String line;
                try {
                    String prompt = PROMPT;
                    String database = queryRunner.getProperties().getProperty("database");
                    if (StringUtils.isNotBlank(database)) {
                        prompt = PROMPT + ":" + database;
                    }
                    String commandPrompt = prompt + ">";
                    line = reader.readLine(colored(commandPrompt), null, remaining);
                } catch (UserInterruptException e) {
                    if (!e.getPartialLine().isEmpty()) {
                        reader.getHistory().add(e.getPartialLine());
                    }
                    remaining = "";
                    continue;
                } catch (EndOfFileException e) {
                    System.out.println();
                    return;
                }
                String command = CharMatcher.is(';').or(whitespace()).trimTrailingFrom(line);
                switch (command.toLowerCase(Locale.ENGLISH)) {
                    case "exit":
                    case "quit":
                        return;
                    case "history":
                        for (History.Entry entry : reader.getHistory()) {
                            System.out.println(new AttributedStringBuilder().style(DEFAULT.foreground(CYAN))
                                .append(format("%5d", entry.index() + 1)).style(DEFAULT).append("  ")
                                .append(entry.line())
                                .toAnsi(reader.getTerminal()));
                        }
                        continue;
                    case "help":
                        System.out.println();
                        System.out.println(getHelpText());
                        continue;
                    default:
                        break;
                }

                StatementSplitter statementSplitter = new StatementSplitter(line, STATEMENT_DELIMITERS);
                for (StatementText split : statementSplitter.getStatements()) {
                    String statement = split.getStatement();
                    if (StatementSplitter.isEmptyStatement(statement)) {
                        continue;
                    }
                    String replace = StringUtils.replace(statement, "\n", " ");
                    process(queryRunner, replace, reader.getTerminal());
                }
                remaining = whitespace().trimTrailingFrom(statementSplitter.getPartialStatement());
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            try (Closer closer = Closer.create()) {
                closer.register(reader.getHistory()::save);
                closer.register(reader.getTerminal());
            } catch (IOException e) {
                e.printStackTrace(System.err);
            }
        }
    }

    /**
     * 获取completer
     *
     * @return
     */
    private Collection<Completer> getCompleters() {
        return ImmutableSet.of(Completion.commandCompleter());
    }

    private static Path getHistoryFile() {
        String path = System.getenv("FASTMODEL_HISTORY_FILE");
        if (StringUtils.isNotBlank(path)) {
            return Paths.get(path);
        }
        return Paths.get(Strings.nullToEmpty(StandardSystemProperty.USER_HOME.value()), ".fastmodel_history");
    }

    private void process(QueryRunner queryRunner, String statement, Terminal terminal) {
        QueryResult execute = null;
        try {
            execute = queryRunner.execute(statement);
            Writer writer = createWriter(System.out);
            if (execute == QueryResult.EMPTY) {
                writer.append("OK").append('\n');
                writer.flush();
            } else {
                OutputPrinter alignedTablePrinter = new AlignedTablePrinter(execute.getColumnInfos(), writer);
                try {
                    alignedTablePrinter.printRows(execute.getRows());
                } finally {
                    alignedTablePrinter.finish();
                }
            }
        } catch (SQLException | IOException | RuntimeException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                System.err.println("Error running command: " + cause.getMessage());
            } else {
                System.err.println("Error running command: " + e.getMessage());
            }
        }
    }

    private static Writer createWriter(OutputStream out) {
        return new BufferedWriter(new OutputStreamWriter(out, UTF_8), 16384);
    }

    private String getHelpText() {
        return Help.getHelpText();
    }

    private static String colored(String value) {
        return new AttributedString(value, DEFAULT.foreground(BRIGHT)).toAnsi();
    }
}
