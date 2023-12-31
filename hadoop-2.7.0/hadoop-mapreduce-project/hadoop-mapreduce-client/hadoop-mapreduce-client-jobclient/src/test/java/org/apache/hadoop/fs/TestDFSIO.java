/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Distributed i/o benchmark.
 * <p>
 * This test writes into or reads from a specified number of files.
 * Number of bytes to write or read is specified as a parameter to the test. 
 * Each file is accessed in a separate map task.
 * <p>
 * The reducer collects the following statistics:
 * <ul>
 * <li>number of tasks completed</li>
 * <li>number of bytes written/read</li>
 * <li>execution time</li>
 * <li>io rate</li>
 * <li>io rate squared</li>
 * </ul>
 *    
 * Finally, the following information is appended to a local file
 * <ul>
 * <li>read or write test</li>
 * <li>date and time the test finished</li>   
 * <li>number of files</li>
 * <li>total number of bytes processed</li>
 * <li>throughput in mb/sec (total number of bytes / sum of processing times)</li>
 * <li>average i/o rate in mb/sec per file</li>
 * <li>standard deviation of i/o rate </li>
 * </ul>
 */
public class TestDFSIO implements Tool {
  // Constants
  private static final Log LOG = LogFactory.getLog(TestDFSIO.class);
  private static final int DEFAULT_BUFFER_SIZE = 1000000;
  private static final String BASE_DIR = "/benchmarks/TestDFSIO";
  private static final String BASE_FILE_NAME = "test_io_";
  private static final String DEFAULT_RES_FILE_NAME = "TestDFSIO_results.log";
  private static final long MEGA = ByteMultiple.MB.value();
  private static final int DEFAULT_NR_BYTES = 128;
  private static final int DEFAULT_NR_FILES = 4;
  private static final String USAGE =
                    "Usage: " + TestDFSIO.class.getSimpleName() +
                    " [genericOptions]" +
                    " -read [-random | -backward | -skip [-skipSize Size]] |" +
                    " -write | -append | -truncate | -clean" +
                    " [-compression codecClassName]" +
                    " [-seq]" +
                    " [-r replVector]" +
                    " [-nrFiles N]" +
                    " [-size Size[B|KB|MB|GB|TB]]" +
                    " [-baseDir baseDir] " +
                    " [-baseFile baseFileName] " +
                    " [-resFile resultFileName] " +
                    " [-bufferSize Bytes]" +
                    " [-rootDir]";

  private Configuration config;

  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  private static enum TestType {
    TEST_TYPE_READ("read"),
    TEST_TYPE_WRITE("write"),
    TEST_TYPE_CLEANUP("cleanup"),
    TEST_TYPE_APPEND("append"),
    TEST_TYPE_READ_RANDOM("random read"),
    TEST_TYPE_READ_BACKWARD("backward read"),
    TEST_TYPE_READ_SKIP("skip read"),
    TEST_TYPE_TRUNCATE("truncate");

    private String type;

    private TestType(String t) {
      type = t;
    }

    @Override // String
    public String toString() {
      return type;
    }
  }

  static enum ByteMultiple {
    B(1L),
    KB(0x400L),
    MB(0x100000L),
    GB(0x40000000L),
    TB(0x10000000000L);

    private long multiplier;

    private ByteMultiple(long mult) {
      multiplier = mult;
    }

    long value() {
      return multiplier;
    }

    static ByteMultiple parseString(String sMultiple) {
      if(sMultiple == null || sMultiple.isEmpty()) // MB by default
        return MB;
      String sMU = StringUtils.toUpperCase(sMultiple);
      if(StringUtils.toUpperCase(B.name()).endsWith(sMU))
        return B;
      if(StringUtils.toUpperCase(KB.name()).endsWith(sMU))
        return KB;
      if(StringUtils.toUpperCase(MB.name()).endsWith(sMU))
        return MB;
      if(StringUtils.toUpperCase(GB.name()).endsWith(sMU))
        return GB;
      if(StringUtils.toUpperCase(TB.name()).endsWith(sMU))
        return TB;
      throw new IllegalArgumentException("Unsupported ByteMultiple "+sMultiple);
    }
  }

  public TestDFSIO() {
    this.config = new Configuration();
  }

  private static String getBaseDir(Configuration conf) {
    return conf.get("test.build.data", BASE_DIR);
  }
  private static Path getControlDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_control");
  }
  private static Path getWriteDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_write");
  }
  private static Path getReadDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_read");
  }
  private static Path getAppendDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_append");
  }
  private static Path getRandomReadDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_random_read");
  }
  private static Path getTruncateDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_truncate");
  }
  private static Path getDataDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_data");
  }
  
  private static long getReplVector(Configuration conf, FileSystem fs, Path path) {
     return conf.getLong("test.io.replication.vector", fs.getDefaultReplication(path));
  }

  private static MiniDFSCluster cluster;
  private static TestDFSIO bench;

  @BeforeClass
  public static void beforeClass() throws Exception {
    bench = new TestDFSIO();
    bench.getConf().setBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY, true);
    bench.getConf().setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    cluster = new MiniDFSCluster.Builder(bench.getConf())
                                .numDataNodes(2)
                                .format(true)
                                .build();
    FileSystem fs = cluster.getFileSystem();
    bench.createControlFile(fs, DEFAULT_NR_BYTES, DEFAULT_NR_FILES);

    /** Check write here, as it is required for other tests */
    testWrite();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if(cluster == null)
      return;
    FileSystem fs = cluster.getFileSystem();
    bench.cleanup(fs);
    cluster.shutdown();
  }

  public static void testWrite() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    long tStart = System.currentTimeMillis();
    bench.writeTest(fs);
    long execTime = System.currentTimeMillis() - tStart;
    bench.analyzeResult(fs, TestType.TEST_TYPE_WRITE, execTime);
  }

  @Test (timeout = 3000)
  public void testRead() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    long tStart = System.currentTimeMillis();
    bench.readTest(fs);
    long execTime = System.currentTimeMillis() - tStart;
    bench.analyzeResult(fs, TestType.TEST_TYPE_READ, execTime);
  }

  @Test (timeout = 3000)
  public void testReadRandom() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    long tStart = System.currentTimeMillis();
    bench.getConf().setLong("test.io.skip.size", 0);
    bench.randomReadTest(fs);
    long execTime = System.currentTimeMillis() - tStart;
    bench.analyzeResult(fs, TestType.TEST_TYPE_READ_RANDOM, execTime);
  }

  @Test (timeout = 3000)
  public void testReadBackward() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    long tStart = System.currentTimeMillis();
    bench.getConf().setLong("test.io.skip.size", -DEFAULT_BUFFER_SIZE);
    bench.randomReadTest(fs);
    long execTime = System.currentTimeMillis() - tStart;
    bench.analyzeResult(fs, TestType.TEST_TYPE_READ_BACKWARD, execTime);
  }

  @Test (timeout = 3000)
  public void testReadSkip() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    long tStart = System.currentTimeMillis();
    bench.getConf().setLong("test.io.skip.size", 1);
    bench.randomReadTest(fs);
    long execTime = System.currentTimeMillis() - tStart;
    bench.analyzeResult(fs, TestType.TEST_TYPE_READ_SKIP, execTime);
  }

  @Test (timeout = 6000)
  public void testAppend() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    long tStart = System.currentTimeMillis();
    bench.appendTest(fs);
    long execTime = System.currentTimeMillis() - tStart;
    bench.analyzeResult(fs, TestType.TEST_TYPE_APPEND, execTime);
  }

  @Test (timeout = 60000)
  public void testTruncate() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    bench.createControlFile(fs, DEFAULT_NR_BYTES / 2, DEFAULT_NR_FILES);
    long tStart = System.currentTimeMillis();
    bench.truncateTest(fs);
    long execTime = System.currentTimeMillis() - tStart;
    bench.analyzeResult(fs, TestType.TEST_TYPE_TRUNCATE, execTime);
  }

  @SuppressWarnings("deprecation")
  private void createControlFile(FileSystem fs,
                                  long nrBytes, // in bytes
                                  int nrFiles
                                ) throws IOException {
    LOG.info("creating control file: "+nrBytes+" bytes, "+nrFiles+" files");

    Path controlDir = getControlDir(config);
    fs.delete(controlDir, true);

    for(int i=0; i < nrFiles; i++) {
      String name = getFileName(config, i);
      Path controlFile = new Path(controlDir, "in_file_" + name);
      SequenceFile.Writer writer = null;
      try {
        writer = SequenceFile.createWriter(fs, config, controlFile,
                                           Text.class, LongWritable.class,
                                           CompressionType.NONE);
        writer.append(new Text(name), new LongWritable(nrBytes));
      } catch(Exception e) {
        throw new IOException(e.getLocalizedMessage());
      } finally {
        if (writer != null)
          writer.close();
        writer = null;
      }
    }
    LOG.info("created control files for: "+nrFiles+" files");
  }

  private static String getFileName(Configuration conf, int fIdx) {
     return conf.get("test.io.base.file.name", BASE_FILE_NAME)
            + Integer.toString(fIdx);
  }
  
  /**
   * Write/Read mapper base class.
   * <p>
   * Collects the following statistics per task:
   * <ul>
   * <li>number of tasks completed</li>
   * <li>number of bytes written/read</li>
   * <li>execution time</li>
   * <li>i/o rate</li>
   * <li>i/o rate squared</li>
   * </ul>
   */
  private abstract static class IOStatMapper extends IOMapperBase<Long> {
    protected CompressionCodec compressionCodec;

    IOStatMapper() {
    }

    @Override // Mapper
    public void configure(JobConf conf) {
      super.configure(conf);

      // grab compression
      String compression = getConf().get("test.io.compression.class", null);
      Class<? extends CompressionCodec> codec;

      // try to initialize codec
      try {
        codec = (compression == null) ? null : 
          Class.forName(compression).asSubclass(CompressionCodec.class);
      } catch(Exception e) {
        throw new RuntimeException("Compression codec not found: ", e);
      }

      if(codec != null) {
        compressionCodec = (CompressionCodec)
            ReflectionUtils.newInstance(codec, getConf());
      }
    }

    @Override // IOMapperBase
    void collectStats(OutputCollector<Text, Text> output, 
                      String name,
                      long execTime, 
                      Long objSize) throws IOException {
      long totalSize = objSize.longValue();
      float ioRateMbSec = (float)totalSize * 1000 / (execTime * MEGA);
      LOG.info("Number of bytes processed = " + totalSize);
      LOG.info("Exec time = " + execTime);
      LOG.info("IO rate = " + ioRateMbSec);
      
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "tasks"),
          new Text(String.valueOf(1)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "size"),
          new Text(String.valueOf(totalSize)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "time"),
          new Text(String.valueOf(execTime)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "rate"),
          new Text(String.valueOf(ioRateMbSec*1000)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "sqrate"),
          new Text(String.valueOf(ioRateMbSec*ioRateMbSec*1000)));
    }
  }

  /**
   * Write mapper class.
   */
  public static class WriteMapper extends IOStatMapper {

    public WriteMapper() { 
      for(int i=0; i < bufferSize; i++)
        buffer[i] = (byte)('0' + i % 50);
    }

    @Override // IOMapperBase
    public Closeable getIOStream(String name) throws IOException {
      // create file
      Path p = new Path(getDataDir(getConf()), name);
      long rv = getReplVector(getConf(), fs, p);
         OutputStream out = fs.create(p, true, bufferSize, rv,
               fs.getDefaultBlockSize(p));
      if(compressionCodec != null)
        out = compressionCodec.createOutputStream(out);
      LOG.info("out = " + out.getClass().getName());
      return out;
    }

    @Override // IOMapperBase
    public Long doIO(Reporter reporter, 
                       String name, 
                       long totalSize // in bytes
                     ) throws IOException {
      OutputStream out = (OutputStream)this.stream;
      // write to the file
      long nrRemaining;
      for (nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize) {
        int curSize = (bufferSize < nrRemaining) ? bufferSize : (int)nrRemaining;
        out.write(buffer, 0, curSize);
        reporter.setStatus("writing " + name + "@" + 
                           (totalSize - nrRemaining) + "/" + totalSize 
                           + " ::host = " + hostName);
      }
      return Long.valueOf(totalSize);
    }
  }

  private void writeTest(FileSystem fs) throws IOException {
    Path writeDir = getWriteDir(config);
    fs.delete(getDataDir(config), true);
    fs.delete(writeDir, true);
    
    runIOTest(WriteMapper.class, writeDir);
  }
  
  private void runIOTest(
          Class<? extends Mapper<Text, LongWritable, Text, Text>> mapperClass, 
          Path outputDir) throws IOException {
    JobConf job = new JobConf(config, TestDFSIO.class);

    FileInputFormat.setInputPaths(job, getControlDir(config));
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(mapperClass);
    job.setReducerClass(AccumulatingReducer.class);

    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }

  /**
   * Append mapper class.
   */
  public static class AppendMapper extends IOStatMapper {

    public AppendMapper() { 
      for(int i=0; i < bufferSize; i++)
        buffer[i] = (byte)('0' + i % 50);
    }

    @Override // IOMapperBase
    public Closeable getIOStream(String name) throws IOException {
      // open file for append
      OutputStream out =
          fs.append(new Path(getDataDir(getConf()), name), bufferSize);
      if(compressionCodec != null)
        out = compressionCodec.createOutputStream(out);
      LOG.info("out = " + out.getClass().getName());
      return out;
    }

    @Override // IOMapperBase
    public Long doIO(Reporter reporter, 
                       String name, 
                       long totalSize // in bytes
                     ) throws IOException {
      OutputStream out = (OutputStream)this.stream;
      // write to the file
      long nrRemaining;
      for (nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize) {
        int curSize = (bufferSize < nrRemaining) ? bufferSize : (int)nrRemaining;
        out.write(buffer, 0, curSize);
        reporter.setStatus("writing " + name + "@" + 
                           (totalSize - nrRemaining) + "/" + totalSize 
                           + " ::host = " + hostName);
      }
      return Long.valueOf(totalSize);
    }
  }

  private void appendTest(FileSystem fs) throws IOException {
    Path appendDir = getAppendDir(config);
    fs.delete(appendDir, true);
    runIOTest(AppendMapper.class, appendDir);
  }

  /**
   * Read mapper class.
   */
  public static class ReadMapper extends IOStatMapper {

    public ReadMapper() { 
    }

    @Override // IOMapperBase
    public Closeable getIOStream(String name) throws IOException {
      // open file
      InputStream in = fs.open(new Path(getDataDir(getConf()), name));
      if(compressionCodec != null)
        in = compressionCodec.createInputStream(in);
      LOG.info("in = " + in.getClass().getName());
      return in;
    }

    @Override // IOMapperBase
    public Long doIO(Reporter reporter, 
                       String name, 
                       long totalSize // in bytes
                     ) throws IOException {
      InputStream in = (InputStream)this.stream;
      long actualSize = 0;
      while (actualSize < totalSize) {
        int curSize = in.read(buffer, 0, bufferSize);
        if(curSize < 0) break;
        actualSize += curSize;
        reporter.setStatus("reading " + name + "@" + 
                           actualSize + "/" + totalSize 
                           + " ::host = " + hostName);
      }
      return Long.valueOf(actualSize);
    }
  }

  private void readTest(FileSystem fs) throws IOException {
    Path readDir = getReadDir(config);
    fs.delete(readDir, true);
    runIOTest(ReadMapper.class, readDir);
  }

  /**
   * Mapper class for random reads.
   * The mapper chooses a position in the file and reads bufferSize
   * bytes starting at the chosen position.
   * It stops after reading the totalSize bytes, specified by -size.
   * 
   * There are three type of reads.
   * 1) Random read always chooses a random position to read from: skipSize = 0
   * 2) Backward read reads file in reverse order                : skipSize < 0
   * 3) Skip-read skips skipSize bytes after every read          : skipSize > 0
   */
  public static class RandomReadMapper extends IOStatMapper {
    private Random rnd;
    private long fileSize;
    private long skipSize;

    @Override // Mapper
    public void configure(JobConf conf) {
      super.configure(conf);
      skipSize = conf.getLong("test.io.skip.size", 0);
    }

    public RandomReadMapper() { 
      rnd = new Random();
    }

    @Override // IOMapperBase
    public Closeable getIOStream(String name) throws IOException {
      Path filePath = new Path(getDataDir(getConf()), name);
      this.fileSize = fs.getFileStatus(filePath).getLen();
      InputStream in = fs.open(filePath);
      if(compressionCodec != null)
        in = new FSDataInputStream(compressionCodec.createInputStream(in));
      LOG.info("in = " + in.getClass().getName());
      LOG.info("skipSize = " + skipSize);
      return in;
    }

    @Override // IOMapperBase
    public Long doIO(Reporter reporter, 
                       String name, 
                       long totalSize // in bytes
                     ) throws IOException {
      PositionedReadable in = (PositionedReadable)this.stream;
      long actualSize = 0;
      for(long pos = nextOffset(-1);
          actualSize < totalSize; pos = nextOffset(pos)) {
        int curSize = in.read(pos, buffer, 0, bufferSize);
        if(curSize < 0) break;
        actualSize += curSize;
        reporter.setStatus("reading " + name + "@" + 
                           actualSize + "/" + totalSize 
                           + " ::host = " + hostName);
      }
      return Long.valueOf(actualSize);
    }

    /**
     * Get next offset for reading.
     * If current < 0 then choose initial offset according to the read type.
     * 
     * @param current offset
     * @return
     */
    private long nextOffset(long current) {
      if(skipSize == 0)
        return rnd.nextInt((int) Math.min(fileSize, Integer.MAX_VALUE));
      if(skipSize > 0)
        return (current < 0) ? 0 : (current + bufferSize + skipSize);
      // skipSize < 0
      return (current < 0) ? Math.max(0, fileSize - bufferSize) :
                             Math.max(0, current + skipSize);
    }
  }

  private void randomReadTest(FileSystem fs) throws IOException {
    Path readDir = getRandomReadDir(config);
    fs.delete(readDir, true);
    runIOTest(RandomReadMapper.class, readDir);
  }

  /**
   * Truncate mapper class.
   * The mapper truncates given file to the newLength, specified by -size.
   */
  public static class TruncateMapper extends IOStatMapper {
    private static final long DELAY = 100L;

    private Path filePath;
    private long fileSize;

    @Override // IOMapperBase
    public Closeable getIOStream(String name) throws IOException {
      filePath = new Path(getDataDir(getConf()), name);
      fileSize = fs.getFileStatus(filePath).getLen();
      return null;
    }

    @Override // IOMapperBase
    public Long doIO(Reporter reporter, 
                       String name, 
                       long newLength // in bytes
                     ) throws IOException {
      boolean isClosed = fs.truncate(filePath, newLength);
      reporter.setStatus("truncating " + name + " to newLength " + 
          newLength  + " ::host = " + hostName);
      for(int i = 0; !isClosed; i++) {
        try {
          Thread.sleep(DELAY);
        } catch (InterruptedException ignored) {}
        FileStatus status = fs.getFileStatus(filePath);
        assert status != null : "status is null";
        isClosed = (status.getLen() == newLength);
        reporter.setStatus("truncate recover for " + name + " to newLength " + 
            newLength + " attempt " + i + " ::host = " + hostName);
      }
      return Long.valueOf(fileSize - newLength);
    }
  }

  private void truncateTest(FileSystem fs) throws IOException {
    Path TruncateDir = getTruncateDir(config);
    fs.delete(TruncateDir, true);
    runIOTest(TruncateMapper.class, TruncateDir);
  }

  private void sequentialTest(TestType testType, 
                              long fileSize, // in bytes
                              int nrFiles,
                              String resFileName
                             ) throws IOException {
     
     long tStart = System.currentTimeMillis();

     // Prepare the map tasks
     Thread[] threads = new Thread[nrFiles];
     AccumulateCollector mapOutput = new AccumulateCollector();
     for (int i = 0; i < nrFiles; ++i) {
        threads[i] = new Thread(new SequentialTestRunnable(
              config, testType, i, fileSize, 
              new BinaryCollector(new SystemOutCollector(""+i), mapOutput), 
              new SystemOutReporter(""+i)));
     }

     // Start the map tasks and wait for them to complete
     for (int i = 0; i < nrFiles; ++i) {
        threads[i].start();
     }

     for (int i = 0; i < nrFiles; ++i) {
        if (threads[i].isAlive()) {
           try {
              threads[i].join();
           } catch (InterruptedException e) {
              LOG.error("Interrupted exception for thread " + i, e);
           }
        }
     }
     
     // Run the reducer
     AccumulateCollector redOutput = new AccumulateCollector();
     BinaryCollector binOutput = new BinaryCollector(new SystemOutCollector("R"), redOutput);
     AccumulatingReducer reducer = new AccumulatingReducer();
     Text key = new Text();
     
     key.set(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "tasks"));
     reducer.reduce(key, mapOutput.getValues(key), binOutput, Reporter.NULL);
     key.set(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "size"));
     reducer.reduce(key, mapOutput.getValues(key), binOutput, Reporter.NULL);
     key.set(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "time"));
     reducer.reduce(key, mapOutput.getValues(key), binOutput, Reporter.NULL);
     key.set(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "rate"));
     reducer.reduce(key, mapOutput.getValues(key), binOutput, Reporter.NULL);
     key.set(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "sqrate"));
     reducer.reduce(key, mapOutput.getValues(key), binOutput, Reporter.NULL);
     
     reducer.close();
     long execTime = System.currentTimeMillis() - tStart;
     
     // Analyze the result
     long tasks = 0;
     long size = 0;
     long time = 0;
     float rate = 0;
     float sqrate = 0;

     key.set(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "tasks"));
     if (redOutput.getValues(key).hasNext())
        tasks = Long.parseLong(redOutput.getValues(key).next().toString());
     
     key.set(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "size"));
     if (redOutput.getValues(key).hasNext())
        size = Long.parseLong(redOutput.getValues(key).next().toString());
     
     key.set(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "time"));
     if (redOutput.getValues(key).hasNext())
        time = Long.parseLong(redOutput.getValues(key).next().toString());
     
     key.set(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "rate"));
     if (redOutput.getValues(key).hasNext())
        rate = Float.parseFloat(redOutput.getValues(key).next().toString());
     
     key.set(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "sqrate"));
     if (redOutput.getValues(key).hasNext())
        sqrate = Float.parseFloat(redOutput.getValues(key).next().toString());     
     
     outputResults(testType, execTime, resFileName, 
           tasks, size, time, rate, sqrate);
  }

  public static void main(String[] args) {
    TestDFSIO bench = new TestDFSIO();
    int res = -1;
    try {
      res = ToolRunner.run(bench, args);
    } catch(Exception e) {
      System.err.print(StringUtils.stringifyException(e));
      res = -2;
    }
    if(res == -1)
      System.err.print(USAGE);
    System.exit(res);
  }

  @Override // Tool
  public int run(String[] args) throws IOException {
    TestType testType = null;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    long nrBytes = 1*MEGA;
    int nrFiles = 1;
    long skipSize = 0;
    String baseFileName = BASE_FILE_NAME;
    String baseDir = BASE_DIR;
    String resFileName = DEFAULT_RES_FILE_NAME;
    String compressionClass = null;
    boolean isSequential = false;
    String version = TestDFSIO.class.getSimpleName() + ".1.8";
    long rv = -1;

    LOG.info(version);
    if (args.length == 0) {
      System.err.println("Missing arguments.");
      return -1;
    }

    for (int i = 0; i < args.length; i++) {       // parse command line
      if (args[i].startsWith("-read")) {
        testType = TestType.TEST_TYPE_READ;
      } else if (args[i].equals("-write")) {
        testType = TestType.TEST_TYPE_WRITE;
      } else if (args[i].equals("-append")) {
        testType = TestType.TEST_TYPE_APPEND;
      } else if (args[i].equals("-random")) {
        if(testType != TestType.TEST_TYPE_READ) return -1;
        testType = TestType.TEST_TYPE_READ_RANDOM;
      } else if (args[i].equals("-backward")) {
        if(testType != TestType.TEST_TYPE_READ) return -1;
        testType = TestType.TEST_TYPE_READ_BACKWARD;
      } else if (args[i].equals("-skip")) {
        if(testType != TestType.TEST_TYPE_READ) return -1;
        testType = TestType.TEST_TYPE_READ_SKIP;
      } else if (args[i].equalsIgnoreCase("-truncate")) {
        testType = TestType.TEST_TYPE_TRUNCATE;
      } else if (args[i].equals("-clean")) {
        testType = TestType.TEST_TYPE_CLEANUP;
      } else if (args[i].startsWith("-seq")) {
        isSequential = true;
      } else if (args[i].equals("-r")) {
        rv = ReplicationVector.ParseReplicationVector(args[++i]);
      } else if (args[i].startsWith("-compression")) {
        compressionClass = args[++i];
      } else if (args[i].equals("-nrFiles")) {
        nrFiles = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-fileSize") || args[i].equals("-size")) {
        nrBytes = parseSize(args[++i]);
      } else if (args[i].equals("-skipSize")) {
        skipSize = parseSize(args[++i]);
      } else if (args[i].equals("-bufferSize")) {
        bufferSize = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-resFile")) {
        resFileName = args[++i];
      } else if (args[i].equals("-baseFile")) {
        baseFileName = args[++i];
      } else if (args[i].equals("-baseDir")) {
        baseDir = args[++i];
      } else {
        System.err.println("Illegal argument: " + args[i]);
        return -1;
      }
    }
    if(testType == null)
      return -1;
    if(testType == TestType.TEST_TYPE_READ_BACKWARD)
      skipSize = -bufferSize;
    else if(testType == TestType.TEST_TYPE_READ_SKIP && skipSize == 0)
      skipSize = bufferSize;

    LOG.info("nrFiles = " + nrFiles);
    LOG.info("nrBytes (MB) = " + toMB(nrBytes));
    LOG.info("bufferSize = " + bufferSize);
    if(skipSize > 0)
      LOG.info("skipSize = " + skipSize);
    
    LOG.info("baseDir = " + baseDir);
    config.set("test.build.data", baseDir);
    
    LOG.info("baseFile = " + baseFileName);
    config.set("test.io.base.file.name", baseFileName);

    if(compressionClass != null) {
      config.set("test.io.compression.class", compressionClass);
      LOG.info("compressionClass = " + compressionClass);
    }
    
    if (rv != -1) {
      config.setLong("test.io.replication.vector", rv);
      LOG.info("replicationVector = " + ReplicationVector.StringifyReplVector(rv));
    }

    config.setInt("test.io.file.buffer.size", bufferSize);
    config.setLong("test.io.skip.size", skipSize);
    config.setBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY, true);
    FileSystem fs = FileSystem.get(config);

    if (testType == TestType.TEST_TYPE_CLEANUP) {
       cleanup(fs);
       return 0;
    }

    if (isSequential) {
      sequentialTest(testType, nrBytes, nrFiles, resFileName);
      return 0;
    }

    createControlFile(fs, nrBytes, nrFiles);
    long tStart = System.currentTimeMillis();
    switch(testType) {
    case TEST_TYPE_WRITE:
      writeTest(fs);
      break;
    case TEST_TYPE_READ:
      readTest(fs);
      break;
    case TEST_TYPE_APPEND:
      appendTest(fs);
      break;
    case TEST_TYPE_READ_RANDOM:
    case TEST_TYPE_READ_BACKWARD:
    case TEST_TYPE_READ_SKIP:
      randomReadTest(fs);
      break;
    case TEST_TYPE_TRUNCATE:
      truncateTest(fs);
      break;
   default:
    }
    long execTime = System.currentTimeMillis() - tStart;
  
    analyzeResult(fs, testType, execTime, resFileName);
    return 0;
  }

  @Override // Configurable
  public Configuration getConf() {
    return this.config;
  }

  @Override // Configurable
  public void setConf(Configuration conf) {
    this.config = conf;
  }

  /**
   * Returns size in bytes.
   * 
   * @param arg = {d}[B|KB|MB|GB|TB]
   * @return
   */
  static long parseSize(String arg) {
    String[] args = arg.split("\\D", 2);  // get digits
    assert args.length <= 2;
    long nrBytes = Long.parseLong(args[0]);
    String bytesMult = arg.substring(args[0].length()); // get byte multiple
    return nrBytes * ByteMultiple.parseString(bytesMult).value();
  }

  static float toMB(long bytes) {
    return ((float)bytes)/MEGA;
  }

  private void analyzeResult( FileSystem fs,
                              TestType testType,
                              long execTime,
                              String resFileName
                            ) throws IOException {
    Path reduceFile = getReduceFilePath(testType);
    long tasks = 0;
    long size = 0;
    long time = 0;
    float rate = 0;
    float sqrate = 0;
    DataInputStream in = null;
    BufferedReader lines = null;
    try {
      in = new DataInputStream(fs.open(reduceFile));
      lines = new BufferedReader(new InputStreamReader(in));
      String line;
      while((line = lines.readLine()) != null) {
        StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
        String attr = tokens.nextToken(); 
        if (attr.endsWith(":tasks"))
          tasks = Long.parseLong(tokens.nextToken());
        else if (attr.endsWith(":size"))
          size = Long.parseLong(tokens.nextToken());
        else if (attr.endsWith(":time"))
          time = Long.parseLong(tokens.nextToken());
        else if (attr.endsWith(":rate"))
          rate = Float.parseFloat(tokens.nextToken());
        else if (attr.endsWith(":sqrate"))
          sqrate = Float.parseFloat(tokens.nextToken());
      }
    } finally {
      if(in != null) in.close();
      if(lines != null) lines.close();
    }
    
    outputResults(testType, execTime, resFileName, 
                  tasks, size, time, rate, sqrate);
  }
  
   private void outputResults(TestType testType, long execTime,
         String resFileName, long tasks, long size, long time, float rate,
         float sqrate) throws FileNotFoundException {
    double med = rate / 1000 / tasks;
    double stdDev = Math.sqrt(Math.abs(sqrate / 1000 / tasks - med*med));
    String resultLines[] = {
      "----- TestDFSIO ----- : " + testType,
      "           Date & time: " + new Date(System.currentTimeMillis()),
      "       Number of files: " + tasks,
      "Total MBytes processed: " + toMB(size),
      "     Throughput mb/sec: " + size * 1000.0 / (time * MEGA),
      "Average IO rate mb/sec: " + med,
      " IO rate std deviation: " + stdDev,
      "    Test exec time sec: " + (float)execTime / 1000,
      "" };

    PrintStream res = null;
    try {
      res = new PrintStream(new FileOutputStream(new File(resFileName), true)); 
      for(int i = 0; i < resultLines.length; i++) {
        LOG.info(resultLines[i]);
        res.println(resultLines[i]);
      }
    } finally {
      if(res != null) res.close();
    }
  }

  private Path getReduceFilePath(TestType testType) {
    switch(testType) {
    case TEST_TYPE_WRITE:
      return new Path(getWriteDir(config), "part-00000");
    case TEST_TYPE_APPEND:
      return new Path(getAppendDir(config), "part-00000");
    case TEST_TYPE_READ:
      return new Path(getReadDir(config), "part-00000");
    case TEST_TYPE_READ_RANDOM:
    case TEST_TYPE_READ_BACKWARD:
    case TEST_TYPE_READ_SKIP:
      return new Path(getRandomReadDir(config), "part-00000");
    case TEST_TYPE_TRUNCATE:
      return new Path(getTruncateDir(config), "part-00000");
    default:
    }
    return null;
  }

  private void analyzeResult(FileSystem fs, TestType testType, long execTime)
      throws IOException {
    String dir = System.getProperty("test.build.dir", "target/test-dir");
    analyzeResult(fs, testType, execTime, dir + "/" + DEFAULT_RES_FILE_NAME);
  }

  private void cleanup(FileSystem fs)
  throws IOException {
    LOG.info("Cleaning up test files");
    fs.delete(new Path(getBaseDir(config)), true);
  }
  
  /**
   * Basic reporter that prints status to System.out
   */
   private static class SystemOutReporter implements Reporter {

      private String id;
      
      public SystemOutReporter(String id) {
         this.id = id;
      }
      
      @Override
      public void progress() {
      }

      @Override
      public void setStatus(String status) {
         System.out.println(id + ": " + status);
      }

      @Override
      public Counter getCounter(Enum<?> name) {
         return null;
      }

      @Override
      public Counter getCounter(String group, String name) {
         return null;
      }

      @Override
      public void incrCounter(Enum<?> key, long amount) {
      }

      @Override
      public void incrCounter(String group, String counter, long amount) {
      }

      @Override
      public InputSplit getInputSplit() throws UnsupportedOperationException {
         throw new UnsupportedOperationException("NULL reporter has no input");
      }

      @Override
      public float getProgress() {
         return 0;
      }
   }
   
   /**
    * Basic collector that prints collected text to System.out
    */
   private static class SystemOutCollector implements
         OutputCollector<Text, Text> {
      
      private String id;
      
      public SystemOutCollector(String id) {
         this.id = id;
      }

      public void collect(Text key, Text value) throws IOException {
         System.out.println(id + ": " + key + " == " + value);
      }
   }

   /**
    * Basic collector that accumulates collected data
    */
   private static class AccumulateCollector implements
         OutputCollector<Text, Text> {
      
      private Map<Text, List<Text>> data;
      
      public AccumulateCollector() {
         this.data = new HashMap<Text, List<Text>>();
      }

      public void collect(Text key, Text value) throws IOException {
         synchronized (data) {
            List<Text> list = data.get(key);
            if (list == null) {
               list = new ArrayList<Text>();
               data.put(key, list);
            }
            list.add(value);
         }
      }
      
      public Iterator<Text> getValues(Text key) {
         List<Text> list = data.get(key);
         if (list != null)
            return list.iterator();
         else
            return new ArrayList<Text>(0).iterator();
      }
   }

   /**
    * Collector for forwarding data to two other collectors
    */
   private static class BinaryCollector implements
         OutputCollector<Text, Text> {
      
      private OutputCollector<Text, Text> col1;
      private OutputCollector<Text, Text> col2;
      
      public BinaryCollector(OutputCollector<Text, Text> col1,
            OutputCollector<Text, Text> col2) {
         this.col1 = col1;
         this.col2 = col2;
      }

      public void collect(Text key, Text value) throws IOException {
         col1.collect(key, value);
         col2.collect(key, value);
      }
   }

   /**
    * Runnable object for running a single map task outside the YARN framework
    */
   private static class SequentialTestRunnable implements Runnable {

      private IOStatMapper ioer;
      private Configuration conf;
      private int id;
      private long fileSize;
      private OutputCollector<Text, Text> output;
      private Reporter reporter;

      public SequentialTestRunnable(Configuration conf, TestType testType,
            int id, long fileSize, OutputCollector<Text, Text> output,
            Reporter reporter) {
         
         switch(testType) {
         case TEST_TYPE_READ:
           ioer = new ReadMapper();
           break;
         case TEST_TYPE_WRITE:
           ioer = new WriteMapper();
           break;
         case TEST_TYPE_APPEND:
           ioer = new AppendMapper();
           break;
         case TEST_TYPE_READ_RANDOM:
         case TEST_TYPE_READ_BACKWARD:
         case TEST_TYPE_READ_SKIP:
           ioer = new RandomReadMapper();
           break;
         case TEST_TYPE_TRUNCATE:
           ioer = new TruncateMapper();
           break;
         default:
            throw new RuntimeException("Invalid test type " + testType);
         }
       
         this.conf = new Configuration(conf);
         this.id = id;
         this.fileSize = fileSize;
         this.output = output;
         this.reporter = reporter;
      }
      
      @Override
      public void run() {
         try {
            JobConf job = new JobConf(conf, TestDFSIO.class);
            job.setInputFormat(SequenceFileInputFormat.class);
            job.setMapperClass(ioer.getClass());
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(0);

            ioer.configure(job);

            Text key = new Text(getFileName(conf, id));
            LongWritable value = new LongWritable(fileSize);

            try {
               ioer.map(key, value, output, reporter);
            } catch (IOException e) {
               LOG.error("Exception executing map " + id, e);
               e.printStackTrace();
            }
         } finally {
            try {
               ioer.close();
            } catch (IOException e) {
               LOG.error("Exception closing map " + id, e);
            }
         }
      }
      
   }
}
