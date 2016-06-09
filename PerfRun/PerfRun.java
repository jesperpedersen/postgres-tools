/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Jesper Pedersen <jesper.pedersen@comcast.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

/**
 * Generate flamegraphs for "perf record" runs
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class PerfRun
{
   // Defaults
   private static String PGSQL_ROOT = "/opt/postgresql-9.6";
   private static String PGSQL_DATA = "/mnt/data/9.6";
   private static String PGSQL_XLOG = "/mnt/xlog/9.6";

   private static String CONFIGURATION = "/home/postgres/Configuration/9.6";
   private static String USER_NAME = "postgres";

   private static long SAMPLE_RATE = 197;
   private static long PERF_RECORD_SLEEP = 60000;

   private static int CONNECTIONS = 100;
   private static int SCALE = 3000;
   private static int RUN_SECONDS = 300;
   private static boolean RUN_TWOPC = true;
   
   /**
    * Start the postmaster process
    */
   private static void startPostgreSQL() throws Exception
   {
      ProcessBuilder pbPostgreSQL =
         new ProcessBuilder(PGSQL_ROOT + "/bin/pg_ctl",
                            "-D",
                            PGSQL_DATA,
                            "-l",
                            PGSQL_DATA + "/logfile",
                            "start");

      pbPostgreSQL.directory(new File("."));

      File log = new File("postgresql.log");
      pbPostgreSQL.redirectErrorStream(true);
      pbPostgreSQL.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process postgresql = pbPostgreSQL.start();
      postgresql.waitFor();

      Thread.sleep(10000L);
   }

   /**
    * Stop the postmaster process
    */
   private static void stopPostgreSQL() throws Exception
   {
      ProcessBuilder pbPostgreSQL =
         new ProcessBuilder(PGSQL_ROOT + "/bin/pg_ctl",
                            "-D",
                            PGSQL_DATA,
                            "-l",
                            PGSQL_DATA + "/logfile",
                            "stop");

      pbPostgreSQL.directory(new File("."));

      File log = new File("postgresql.log");
      pbPostgreSQL.redirectErrorStream(true);
      pbPostgreSQL.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process postgresql = pbPostgreSQL.start();
      postgresql.waitFor();

      Thread.sleep(30000L);
   }

   /**
    * Recursive delete directory
    * @param dir The directory
    */ 
   private static void purgeDirectory(File dir) throws Exception
   {
      for (File file: dir.listFiles())
      {
         if (file.isDirectory())
            purgeDirectory(file);
         file.delete();
      }
   }
   
   /**
    * Configure PostgreSQL
    */
   private static void configurePostgreSQL() throws Exception
   {
      File dataDir = new File(PGSQL_DATA + "/");
      File xlogDir = new File(PGSQL_XLOG + "/");
      
      purgeDirectory(dataDir);
      purgeDirectory(xlogDir);

      dataDir.mkdirs();
      xlogDir.mkdirs();
      
      ProcessBuilder pbInitdb =
         new ProcessBuilder(PGSQL_ROOT + "/bin/initdb",
                            "-D",
                            PGSQL_DATA,
                            "-X",
                            PGSQL_XLOG);

      pbInitdb.directory(new File("."));

      File log = new File("configure.log");
      pbInitdb.redirectErrorStream(true);
      pbInitdb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process initdb = pbInitdb.start();
      initdb.waitFor();

      File[] files = new File(CONFIGURATION + "/").listFiles();
      String path = PGSQL_DATA + "/";
      for (File file : files)
      {
         if (file.isFile())
            Files.copy(file.toPath(), (new File(path + file.getName())).toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
   }
   
   /**
    * Change configuration to use synchronous commit
    */
   private static void synchronousCommit() throws Exception
   {
      ProcessBuilder pbSC =
         new ProcessBuilder("sed",
                            "-i",
                            "s/synchronous_commit = off/synchronous_commit = on/g",
                            PGSQL_DATA + "/postgresql.conf");

      pbSC.directory(new File("."));

      File log = new File("configure.log");
      pbSC.redirectErrorStream(true);
      pbSC.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process sed = pbSC.start();
      sed.waitFor();
   }
   
   /**
    * Init pgbench with logged tables
    */
   private static void initLogged() throws Exception
   {
      ProcessBuilder pbCreateDB =
         new ProcessBuilder(PGSQL_ROOT + "/bin/createdb",
                            "-E",
                            "UTF8",
                            "pgbench");

      pbCreateDB.directory(new File("."));

      File log = new File("configure.log");
      pbCreateDB.redirectErrorStream(true);
      pbCreateDB.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process createdb = pbCreateDB.start();
      createdb.waitFor();

      ProcessBuilder pbPgBench =
         new ProcessBuilder(PGSQL_ROOT + "/bin/pgbench",
                            "-i",
                            "-s",
                            Integer.toString(SCALE),
                            "-q",
                            "pgbench");

      pbPgBench.directory(new File("."));

      pbPgBench.redirectErrorStream(true);
      pbPgBench.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process pgbench = pbPgBench.start();
      pgbench.waitFor();
   }
   
   /**
    * Init pgbench with unlogged tables
    */
   private static void initUnlogged() throws Exception
   {
      ProcessBuilder pbCreateDB =
         new ProcessBuilder(PGSQL_ROOT + "/bin/createdb",
                            "-E",
                            "UTF8",
                            "pgbench");

      pbCreateDB.directory(new File("."));

      File log = new File("configure.log");
      pbCreateDB.redirectErrorStream(true);
      pbCreateDB.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process createdb = pbCreateDB.start();
      createdb.waitFor();

      ProcessBuilder pbPgBench =
         new ProcessBuilder(PGSQL_ROOT + "/bin/pgbench",
                            "-i",
                            "-s",
                            Integer.toString(SCALE),
                            "-q",
                            "--unlogged-tables",
                            "pgbench");

      pbPgBench.directory(new File("."));

      pbPgBench.redirectErrorStream(true);
      pbPgBench.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process pgbench = pbPgBench.start();
      pgbench.waitFor();
   }
   
   /**
    * 1PC: Standard
    */
   private static void run1PCStandard() throws Exception
   {
      ProcessBuilder pb =
         new ProcessBuilder(PGSQL_ROOT + "/bin/pgbench",
                            "-c",
                            Integer.toString(CONNECTIONS),
                            "-j",
                            Integer.toString(CONNECTIONS),
                            "-T",
                            Integer.toString(RUN_SECONDS),
                            "-U",
                            USER_NAME,
                            "pgbench");

      pb.directory(new File("."));

      File log = new File("run.log");
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process pgbench = pb.start();
      pgbench.waitFor();
   }
   
   /**
    * 1PC: Prepared (-M prepared)
    */
   private static void run1PCPrepared() throws Exception
   {
      ProcessBuilder pb =
         new ProcessBuilder(PGSQL_ROOT + "/bin/pgbench",
                            "-c",
                            Integer.toString(CONNECTIONS),
                            "-j",
                            Integer.toString(CONNECTIONS),
                            "-M",
                            "prepared",
                            "-T",
                            Integer.toString(RUN_SECONDS),
                            "-U",
                            USER_NAME,
                            "pgbench");

      pb.directory(new File("."));

      File log = new File("run.log");
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process pgbench = pb.start();
      pgbench.waitFor();
   }
   
   /**
    * RO (-S)
    */
   private static void runReadOnly() throws Exception
   {
      ProcessBuilder pb =
         new ProcessBuilder(PGSQL_ROOT + "/bin/pgbench",
                            "-c",
                            Integer.toString(CONNECTIONS),
                            "-j",
                            Integer.toString(CONNECTIONS),
                            "-S",
                            "-T",
                            Integer.toString(RUN_SECONDS),
                            "-U",
                            USER_NAME,
                            "pgbench");

      pb.directory(new File("."));

      File log = new File("run.log");
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process pgbench = pb.start();
      pgbench.waitFor();
   }
   
   /**
    * 2PC: Standard
    */
   private static void run2PCStandard() throws Exception
   {
      ProcessBuilder pb =
         new ProcessBuilder(PGSQL_ROOT + "/bin/pgbench",
                            "-c",
                            Integer.toString(CONNECTIONS),
                            "-j",
                            Integer.toString(CONNECTIONS),
                            "-X",
                            "-T",
                            Integer.toString(RUN_SECONDS),
                            "-U",
                            USER_NAME,
                            "pgbench");

      pb.directory(new File("."));

      File log = new File("run.log");
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process pgbench = pb.start();
      pgbench.waitFor();
   }

   /**
    * Start perf record
    */
   private static Process startPerfRecord() throws Exception
   {
      ProcessBuilder pb =
         new ProcessBuilder("perf", "record", "-F", Long.toString(SAMPLE_RATE), "-g", "-a");

      pb.directory(new File("."));

      Files.deleteIfExists(Paths.get("perf-record.log"));
      
      File log = new File("perf-record.log");
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      return pb.start();
   }

   /**
    * Stop perf record
    * @param p The process
    */
   private static void stopPerfRecord(Process p) throws Exception
   {
      p.destroy();

      Thread.sleep(PERF_RECORD_SLEEP);
   }

   /**
    * Run perf script
    */
   private static void perfScript() throws Exception
   {
      ProcessBuilder pb =
         new ProcessBuilder("perf", "script", "-i", "perf.data");

      pb.directory(new File("."));

      Files.deleteIfExists(Paths.get("script.perf"));
      Files.deleteIfExists(Paths.get("script.error"));
      
      File output = new File("script.perf");
      pb.redirectOutput(output);

      File error = new File("script.error");
      pb.redirectError(error);

      Process p = pb.start();
      p.waitFor();
   }

   /**
    * Run stackcollapse-perf.pl
    */
   private static void stackCollapse(String name) throws Exception
   {
      ProcessBuilder pbStackCollapse =
         new ProcessBuilder("perl", "stackcollapse-perf.pl", "script.perf");

      pbStackCollapse.directory(new File("."));

      Files.deleteIfExists(Paths.get("stackcollapse.perf"));
      Files.deleteIfExists(Paths.get("stackcollapse.error"));

      File output = new File("stackcollapse.perf");
      pbStackCollapse.redirectOutput(output);

      File error = new File("stackcollapse.error");
      pbStackCollapse.redirectError(error);

      Process stackcollapse = pbStackCollapse.start();
      stackcollapse.waitFor();

      ProcessBuilder pbGrep =
         new ProcessBuilder("grep", "postgres", "stackcollapse.perf");

      pbGrep.directory(new File("."));

      Files.deleteIfExists(Paths.get(name + ".perf"));
      Files.deleteIfExists(Paths.get(name + ".error"));

      output = new File(name + ".perf");
      pbGrep.redirectOutput(output);

      error = new File(name + ".error");
      pbGrep.redirectError(error);

      Process grep = pbGrep.start();
      grep.waitFor();
   }

   /**
    * Generate a flamegraph
    * @param name The name of the profile
    */
   private static void flamegraph(String name) throws Exception
   {
      ProcessBuilder pb =
         new ProcessBuilder("perl", "flamegraph.pl", "--title=\'" + name + "\'", name + ".perf");

      pb.directory(new File("."));

      Files.deleteIfExists(Paths.get(name + ".svg"));
      Files.deleteIfExists(Paths.get(name + ".error"));

      File output = new File(name + ".svg");
      pb.redirectOutput(output);

      File error = new File(name + ".error");
      pb.redirectError(error);

      Process p = pb.start();
      p.waitFor();

      if (Files.size(Paths.get(name + ".error")) == 0)
         Files.deleteIfExists(Paths.get(name + ".error"));
   }

   /**
    * Move and compress files
    * @param name The name of the profile
    */
   private static void moveFiles(String name) throws Exception
   {
      File directory = new File("result");
      directory.mkdirs();

      Files.deleteIfExists(Paths.get("result", name + ".perf.gz"));
      compressGzipFile(name + ".perf", "result/" + name + ".perf.gz");
      Files.deleteIfExists(Paths.get(name + ".perf"));

      Files.move(Paths.get(name + ".svg"), Paths.get("result", name + ".svg"),
                 StandardCopyOption.REPLACE_EXISTING);
   }

   /**
    * Compress a file
    * @param file The name of the file
    * @param gzipFile The name of the compressed file
    */
   private static void compressGzipFile(String file, String gzipFile) throws Exception
   {
      FileInputStream fis = null;
      FileOutputStream fos = null;
      GZIPOutputStream gzipOS = null;
      try
      {
         fis = new FileInputStream(file);
         fos = new FileOutputStream(gzipFile);
         gzipOS = new GZIPOutputStream(fos);

         byte[] buffer = new byte[8192];
         int len;

         while ((len = fis.read(buffer)) != -1)
         {
            gzipOS.write(buffer, 0, len);
         }
      }
      finally
      {
         if (gzipOS != null)
         {
            try
            {
               gzipOS.close();
            }
            catch (IOException ignore)
            {
               // Ignore
            }
         }
         if (fos != null)
         {
            try
            {
               fos.close();
            }
            catch (IOException ignore)
            {
               // Ignore
            }
         }
         if (fis != null)
         {
            try
            {
               fis.close();
            }
            catch (IOException ignore)
            {
               // Ignore
            }
         }
      }
   }

   /**
    * Off / Logged run
    */
   private static void offLogged() throws Exception
   {
      Process perfRecord = null;

      // 1PC
      stopPostgreSQL();
      configurePostgreSQL();
      startPostgreSQL();
      initLogged();

      // Warm up
      run1PCStandard();
      
      perfRecord = startPerfRecord();
      run1PCStandard();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("off-logged-1pc-standard");
      flamegraph("off-logged-1pc-standard");
      moveFiles("off-logged-1pc-standard");
      
      // 1PCP
      stopPostgreSQL();
      configurePostgreSQL();
      startPostgreSQL();
      initLogged();
      
      // Warm up
      run1PCPrepared();
      
      perfRecord = startPerfRecord();
      run1PCPrepared();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("off-logged-1pc-prepared");
      flamegraph("off-logged-1pc-prepared");
      moveFiles("off-logged-1pc-prepared");
      
      // RO
      stopPostgreSQL();
      configurePostgreSQL();
      startPostgreSQL();
      initLogged();
      
      // Warm up
      runReadOnly();
      
      perfRecord = startPerfRecord();
      runReadOnly();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("off-logged-read-only");
      flamegraph("off-logged-read-only");
      moveFiles("off-logged-read-only");
      
      // 2PC
      if (RUN_TWOPC)
      {
         stopPostgreSQL();
         configurePostgreSQL();
         startPostgreSQL();
         initLogged();
      
         // Warm up
         run2PCStandard();
      
         perfRecord = startPerfRecord();
         run2PCStandard();
         stopPerfRecord(perfRecord);
      
         perfScript();
         stackCollapse("off-logged-2pc-standard");
         flamegraph("off-logged-2pc-standard");
         moveFiles("off-logged-2pc-standard");
      }
   }
   
   /**
    * Off / Unlogged run
    */
   private static void offUnlogged() throws Exception
   {
      Process perfRecord = null;

      // 1PC
      stopPostgreSQL();
      configurePostgreSQL();
      startPostgreSQL();
      initUnlogged();
      
      // Warm up
      run1PCStandard();
      
      perfRecord = startPerfRecord();
      run1PCStandard();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("off-unlogged-1pc-standard");
      flamegraph("off-unlogged-1pc-standard");
      moveFiles("off-unlogged-1pc-standard");
      
      // 1PCP
      stopPostgreSQL();
      configurePostgreSQL();
      startPostgreSQL();
      initUnlogged();
      
      // Warm up
      run1PCPrepared();
      
      perfRecord = startPerfRecord();
      run1PCPrepared();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("off-unlogged-1pc-prepared");
      flamegraph("off-unlogged-1pc-prepared");
      moveFiles("off-unlogged-1pc-prepared");
      
      // RO
      stopPostgreSQL();
      configurePostgreSQL();
      startPostgreSQL();
      initUnlogged();
      
      // Warm up
      runReadOnly();
      
      perfRecord = startPerfRecord();
      runReadOnly();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("off-unlogged-read-only");
      flamegraph("off-unlogged-read-only");
      moveFiles("off-unlogged-read-only");
      
      // 2PC
      if (RUN_TWOPC)
      {
         stopPostgreSQL();
         configurePostgreSQL();
         startPostgreSQL();
         initUnlogged();
      
         // Warm up
         run2PCStandard();
      
         perfRecord = startPerfRecord();
         run2PCStandard();
         stopPerfRecord(perfRecord);
      
         perfScript();
         stackCollapse("off-unlogged-2pc-standard");
         flamegraph("off-unlogged-2pc-standard");
         moveFiles("off-unlogged-2pc-standard");
      }
   }
   
   /**
    * On / Logged run
    */
   private static void onLogged() throws Exception
   {
      Process perfRecord = null;

      // 1PC
      stopPostgreSQL();
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initLogged();
      
      // Warm up
      run1PCStandard();
      
      perfRecord = startPerfRecord();
      run1PCStandard();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("on-logged-1pc-standard");
      flamegraph("on-logged-1pc-standard");
      moveFiles("on-logged-1pc-standard");
      
      // 1PCP
      stopPostgreSQL();
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initLogged();
      
      // Warm up
      run1PCPrepared();
      
      perfRecord = startPerfRecord();
      run1PCPrepared();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("on-logged-1pc-prepared");
      flamegraph("on-logged-1pc-prepared");
      moveFiles("on-logged-1pc-prepared");
      
      // RO
      stopPostgreSQL();
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initLogged();
      
      // Warm up
      runReadOnly();
      
      perfRecord = startPerfRecord();
      runReadOnly();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("on-logged-read-only");
      flamegraph("on-logged-read-only");
      moveFiles("on-logged-read-only");
      
      // 2PC
      if (RUN_TWOPC)
      {
         stopPostgreSQL();
         configurePostgreSQL();
         synchronousCommit();
         startPostgreSQL();
         initLogged();
      
         // Warm up
         run2PCStandard();
      
         perfRecord = startPerfRecord();
         run2PCStandard();
         stopPerfRecord(perfRecord);
      
         perfScript();
         stackCollapse("on-logged-2pc-standard");
         flamegraph("on-logged-2pc-standard");
         moveFiles("on-logged-2pc-standard");
      }
   }

   /**
    * On / Unlogged run
    */
   private static void onUnlogged() throws Exception
   {
      Process perfRecord = null;

      // 1PC
      stopPostgreSQL();
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initUnlogged();
      
      // Warm up
      run1PCStandard();
      
      perfRecord = startPerfRecord();
      run1PCStandard();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("on-unlogged-1pc-standard");
      flamegraph("on-unlogged-1pc-standard");
      moveFiles("on-unlogged-1pc-standard");
      
      // 1PCP
      stopPostgreSQL();
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initUnlogged();
      
      // Warm up
      run1PCPrepared();
      
      perfRecord = startPerfRecord();
      run1PCPrepared();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("on-unlogged-1pc-prepared");
      flamegraph("on-unlogged-1pc-prepared");
      moveFiles("on-unlogged-1pc-prepared");
      
      // RO
      stopPostgreSQL();
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initUnlogged();
      
      // Warm up
      runReadOnly();
      
      perfRecord = startPerfRecord();
      runReadOnly();
      stopPerfRecord(perfRecord);
      
      perfScript();
      stackCollapse("on-unlogged-read-only");
      flamegraph("on-unlogged-read-only");
      moveFiles("on-unlogged-read-only");
      
      // 2PC
      if (RUN_TWOPC)
      {
         stopPostgreSQL();
         configurePostgreSQL();
         synchronousCommit();
         startPostgreSQL();
         initUnlogged();
      
         // Warm up
         run2PCStandard();
      
         perfRecord = startPerfRecord();
         run2PCStandard();
         stopPerfRecord(perfRecord);
      
         perfScript();
         stackCollapse("on-unlogged-2pc-standard");
         flamegraph("on-unlogged-2pc-standard");
         moveFiles("on-unlogged-2pc-standard");
      }
   }

   /**
    * Read the configuration (lwlockrun.properties)
    */
   private static void readConfiguration() throws Exception
   {
      File f = new File("perfrun.properties");

      if (f.exists())
      {
         Properties p = new Properties();
         FileInputStream fis = null;
         try
         {
            fis = new FileInputStream(f);
            p.load(fis);
         }
         finally
         {
            if (fis != null)
            {
               try
               {
                  fis.close();
               }
               catch (Exception e)
               {
                  // Nothing todo
               }
            }
         }

         PGSQL_ROOT = p.getProperty("PGSQL_ROOT", PGSQL_ROOT);
         PGSQL_DATA = p.getProperty("PGSQL_DATA", PGSQL_DATA);
         PGSQL_XLOG = p.getProperty("PGSQL_XLOG", PGSQL_XLOG);

         CONFIGURATION = p.getProperty("CONFIGURATION", CONFIGURATION);

         USER_NAME = p.getProperty("USER_NAME", USER_NAME);

         CONNECTIONS = Integer.valueOf(p.getProperty("CONNECTIONS", Integer.toString(CONNECTIONS)));
         SCALE = Integer.valueOf(p.getProperty("SCALE", Integer.toString(SCALE)));
         RUN_SECONDS = Integer.valueOf(p.getProperty("RUN_SECONDS", Integer.toString(RUN_SECONDS)));

         SAMPLE_RATE = Long.valueOf(p.getProperty("SAMPLE_RATE", Long.toString(SAMPLE_RATE)));
         PERF_RECORD_SLEEP = Long.valueOf(p.getProperty("PERF_RECORD_SLEEP", Long.toString(PERF_RECORD_SLEEP)));

         RUN_TWOPC = Boolean.valueOf(p.getProperty("RUN_TWOPC", Boolean.toString(RUN_TWOPC)));
      }
   }

   /**
    * Main
    */
   public static void main(String[] args)
   {
      try
      {
         readConfiguration();

         offLogged();
         offUnlogged();

         onLogged();
         onUnlogged();

         // Cleanup
         Files.deleteIfExists(Paths.get("perf.data"));
         Files.deleteIfExists(Paths.get("perf.data.old"));

         System.exit(0);
      }
      catch (Exception e)
      {
         try
         {
            File error = new File("error.err");
            PrintWriter errorPW = new PrintWriter(error);
            errorPW.print(e.getMessage());
            errorPW.println();
            e.printStackTrace(errorPW);
            errorPW.flush();
            errorPW.close();
         }
         catch (Exception ie)
         {
            ie.printStackTrace();
         }

         System.exit(1);
      }
   }
}
