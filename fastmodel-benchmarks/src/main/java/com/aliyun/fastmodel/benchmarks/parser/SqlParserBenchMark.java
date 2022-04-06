package com.aliyun.fastmodel.benchmarks.parser;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.management.MXBean;

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * 用于分割字符的性能测试程序
 *
 * @author panguanjing
 */

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@Threads(10)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SqlParserBenchMark {

    FastModelParser fastModelParser;

    @Setup
    public void setup() {
        fastModelParser = FastModelParserFactory.getInstance().get();
    }

    @TearDown
    public void tearDown() throws InterruptedException {
    }

    @Benchmark
    public void druidParser() throws IOException {

    }

    @Benchmark
    public void fmlParser() {
        fastModelParser.parseStatement("create dim table a (b bigint) comment 'abc';");
    }

}
