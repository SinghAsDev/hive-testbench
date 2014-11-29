package org.notmysock.tpcds;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.MessageDigest;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenTable extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new GenTable(), args);
    System.exit(res);
  }

  @SuppressWarnings("deprecation")
  @Override
  public int run(String[] args) throws Exception {
    String[] remainingArgs = new GenericOptionsParser(getConf(), args)
        .getRemainingArgs();

    CommandLineParser parser = new BasicParser();
    getConf().setInt("io.sort.mb", 4);
    org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
    options.addOption("s", "scale", true, "scale");
    options.addOption("t", "table", true, "table");
    options.addOption("d", "dir", true, "dir");
    options.addOption("p", "parallel", true, "parallel");
    CommandLine line = parser.parse(options, remainingArgs);
    if (!(line.hasOption("scale") && line.hasOption("dir"))) {
      HelpFormatter f = new HelpFormatter();
      f.printHelp("GenTable", options);
      return 1;
    }
    int scale = Integer.parseInt(line.getOptionValue("scale"));
    if (!line.hasOption("table")) {
      throw new IllegalArgumentException("Table is required");
    }
    String table = line.getOptionValue("table");
    Path out = new Path(line.getOptionValue("dir"), table);
    int parallel = scale;
    if (line.hasOption("parallel")) {
      parallel = Integer.parseInt(line.getOptionValue("parallel"));
    }
    if (parallel == 1 || scale == 1) {
      System.err.println("The MR task does not work for scale=1 or parallel=1");
      return 1;
    }
    FileSystem fs = FileSystem.get(getConf());
    Path in = genInput(table, scale, parallel);
    fs.delete(out, true);
    Path dsdgen = copyJar(new File("target/lib/dsdgen.jar"));
    URI dsuri = dsdgen.toUri();
    URI link = new URI(dsuri.getScheme(), dsuri.getUserInfo(), dsuri.getHost(),
        dsuri.getPort(), dsuri.getPath(), dsuri.getQuery(), "dsdgen");
    Configuration conf = getConf();
    conf.setInt("mapred.task.timeout", 0);
    conf.setInt("mapreduce.task.timeout", 0);
    conf.setBoolean("mapreduce.map.output.compress", true);
    conf.set("mapreduce.map.output.compress.codec",
        "org.apache.hadoop.io.compress.GzipCodec");
    DistributedCache.addCacheArchive(link, conf);
    DistributedCache.createSymlink(conf);
    Job job = new Job(conf, "GenTable+" + table + "_" + scale);
    job.setJarByClass(getClass());
    job.setNumReduceTasks(0);
    job.setMapperClass(DSDGen.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.setNumLinesPerSplit(job, 1);

    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);

    // use multiple output to only write the named files
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    boolean success = job.waitForCompletion(true);
    // cleanup
    fs.delete(in, false);
    fs.delete(dsdgen, false);

    return success ? 0 : 1;
  }

  private Path copyJar(File jar) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    InputStream is = new FileInputStream(jar);
    try {
      is = new DigestInputStream(is, md);
      // read stream to EOF as normal...
    } finally {
      is.close();
    }
    BigInteger md5 = new BigInteger(md.digest());
    String md5hex = md5.toString(16);
    Path dst = new Path(String.format("/tmp/%s.jar", md5hex));
    Path src = new Path(jar.toURI());
    FileSystem fs = FileSystem.get(getConf());
    fs.copyFromLocalFile(false, /* overwrite */true, src, dst);
    return dst;
  }

  private Path genInput(String table, int scale, int parallel) throws Exception {
    long epoch = System.currentTimeMillis() / 1000;

    Path in = new Path("/tmp/" + table + "_" + scale + "-" + epoch);
    FileSystem fs = FileSystem.get(getConf());
    FSDataOutputStream out = fs.create(in);
    for (int i = 1; i <= parallel; i++) {
      if (table.equals("all")) {
        out.writeBytes(String.format("all %d %d %d\n", scale, parallel, i));
        // out.writeBytes(String.format("./dsdgen -dir $DIR -force Y -scale %d -parallel %d -child %d\n",
        // scale, parallel, i));
      } else {
        out.writeBytes(String
            .format("%s %d %d %d\n", table, scale, parallel, i));
        // out.writeBytes(String.format("./dsdgen -dir $DIR -table %s -force Y -scale %d -parallel %d -child %d\n",
        // table, scale, parallel, i));
      }
    }
    out.close();
    return in;
  }

  static final class DSDGen extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable offset, Text command, Context context)
        throws IOException, InterruptedException {
      String[] args = command.toString().split(" ");
      String table = args[0];
      String scale = args[1];
      String parallel = args[2];
      String child = args[3];
      String cmd = String.format("./dsdgen -TABLE %s -FORCE Y -SCALE %s -PARALLEL %s -CHILD %s -QUIET Y -FILTER Y",
          table, scale, parallel, child);
      System.out.println(cmd);
      final Process p = Runtime.getRuntime().exec(cmd, null,
          new File("dsdgen/tools/"));
      
      Thread stderrReader = new Thread() {
        public void run() {
          try {
            InputStreamReader is = new InputStreamReader(p.getErrorStream());
            BufferedReader br = new BufferedReader(is);
            String read;
            while ((read = br.readLine()) != null) {
              System.err.println(read);
            }
          } catch (EOFException e) {
            // ignore
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };
      stderrReader.setName("StderrReader");
      stderrReader.setDaemon(true);
      stderrReader.start();
      BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line;
      Text textLine = new Text();
      while ((line = br.readLine()) != null) {
        textLine.set(line);
        context.write(textLine, null);
      }
      br.close();
      int status = p.waitFor();
      if (status != 0) {
        throw new InterruptedException("Process failed with status code " + status);
      }
    }
  }
}
