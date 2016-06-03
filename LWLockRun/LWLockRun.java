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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

/**
 * Generate flamegraphs for LWLock usage
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class LWLockRun
{
   // Defaults
   private static String PGSQL_ROOT = "/opt/postgresql-9.6";
   private static String PGSQL_DATA = "/mnt/data/9.6";
   private static String PGSQL_XLOG = "/mnt/xlog/9.6";

   private static String CONFIGURATION = "/home/postgres/Configuration/9.6/";
   private static String USER_NAME = "postgres";
   private static int CONNECTIONS = 100;
   private static int SCALE = 3000;
   private static int RUN_SECONDS = 300;
   private static long SLEEP_TIME = 300000L;
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

      Files.deleteIfExists(Paths.get("postgresql.out"));
      
      File log = new File("postgresql.out");
      pbPostgreSQL.redirectErrorStream(true);
      pbPostgreSQL.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process postgresql = pbPostgreSQL.start();
      postgresql.waitFor();

      Thread.sleep(10000L);
   }

   /**
    * Stop the postmaster process (quick)
    */
   private static void stopFastPostgreSQL() throws Exception
   {
      ProcessBuilder pbPostgreSQL =
         new ProcessBuilder(PGSQL_ROOT + "/bin/pg_ctl",
                            "-D",
                            PGSQL_DATA,
                            "-l",
                            PGSQL_DATA + "/logfile",
                            "stop");

      pbPostgreSQL.directory(new File("."));

      Files.deleteIfExists(Paths.get("postgresql.out"));
      
      File log = new File("postgresql.out");
      pbPostgreSQL.redirectErrorStream(true);
      pbPostgreSQL.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process postgresql = pbPostgreSQL.start();
      postgresql.waitFor();

      Thread.sleep(30000L);
   }

   /**
    * Stop the postmaster process (slow)
    *
    * Needed in order to have the LWLock information written out in the log file
    */
   private static void stopSlowPostgreSQL() throws Exception
   {
      ProcessBuilder pbPostgreSQL =
         new ProcessBuilder(PGSQL_ROOT + "/bin/pg_ctl",
                            "-D",
                            PGSQL_DATA,
                            "-l",
                            PGSQL_DATA + "/logfile",
                            "stop");

      pbPostgreSQL.directory(new File("."));

      Files.deleteIfExists(Paths.get("postgresql.out"));
      
      File log = new File("postgresql.out");
      pbPostgreSQL.redirectErrorStream(true);
      pbPostgreSQL.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process postgresql = pbPostgreSQL.start();
      postgresql.waitFor();

      Thread.sleep(SLEEP_TIME);
   }

   /**
    * Delete existing log file
    */ 
   private static void removeLog() throws Exception
   {
      Files.deleteIfExists(Paths.get(PGSQL_DATA + "/logfile"));
   }
   
   /**
    * Recursive delete directory
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

      File log = new File("configure.out");
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

      File log = new File("configure.out");
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

      File log = new File("configure.out");
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

      File log = new File("configure.out");
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

      File log = new File("run.out");
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

      File log = new File("run.out");
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

      File log = new File("run.out");
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

      File log = new File("run.out");
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));

      Process pgbench = pb.start();
      pgbench.waitFor();
   }

   /**
    * Copy the log file
    * @param name The new name of the log file
    */
   private static void copyLogfile(String name) throws Exception
   {
      Files.copy(Paths.get(PGSQL_DATA + "/logfile"), Paths.get(name + ".log"),
                 StandardCopyOption.REPLACE_EXISTING);
   }

   /**
    * Process the log file for LWLock information
    * 
    * Creates the flamegraph data files for the profile in question
    * @param profile The name of the profile
    */
   public static void processLog(String profile) throws Exception
   {
      String data = null;

      Map<String, Map<String, Long>> m
         = new TreeMap<String, Map<String, Long>>();
      
      Map<String, String> names = new TreeMap<String, String>();
      
      File input = new File(profile + ".log");
      FileReader fr = new FileReader(input);
      LineNumberReader lnr = new LineNumberReader(fr, 8192);

      data = lnr.readLine();
      while (data != null)
      {
         if (data.startsWith("PID"))
         {
            StringTokenizer st = new StringTokenizer(data, " ");

            String pid = st.nextToken();
            String process = st.nextToken();
            String lwlock = st.nextToken();

            String name = st.nextToken();
            String id = "";

            if (!name.endsWith(":"))
            {
               String last = "";
               while (!last.endsWith(":"))
               {
                  last = st.nextToken();
                  if (!last.endsWith(":"))
                  {
                     name += " ";
                     name += last;
                  }
               }
               
               id = last.substring(0, last.length() - 1);
            }
            else
            {
               name = name.substring(0, name.length() - 1);
            }

            String shacq = st.nextToken();
            String shacqValue = st.nextToken();
            
            String shmax = st.nextToken();
            String shmaxValue = st.nextToken();
            
            String exacq = st.nextToken();
            String exacqValue = st.nextToken();
            
            String exmax = st.nextToken();
            String exmaxValue = st.nextToken();
            
            String blk = st.nextToken();
            String blkValue = st.nextToken();
            
            String spindelay = st.nextToken();
            String spindelayValue = st.nextToken();
            
            String deque = st.nextToken(); st.nextToken();
            String dequeValue = st.nextToken();
            
            String maxw = st.nextToken();
            String maxwValue = st.nextToken();

            String key = name;
            if (!id.equals(""))
               key += " " + id;
            
            Map<String, Long> entry = m.get(key);
            if (entry == null)
               entry = new TreeMap<String, Long>();

            // SHARED LOCK
            Long l = entry.get(shacq);
            if (l == null)
               l = Long.valueOf(shacqValue);
            else
               l = Long.valueOf(l.longValue() + Long.valueOf(shacqValue).longValue());
            entry.put(shacq, l);
            
            l = entry.get(shmax);
            if (l == null)
               l = Long.valueOf(shmaxValue);
            else
            {
               Long t = Long.valueOf(shmaxValue);
               if (t.longValue() > l.longValue())
                  l = t;
            }
            entry.put(shmax, l);

            // EXCLUSIVE LOCK
            l = entry.get(exacq);
            if (l == null)
               l = Long.valueOf(exacqValue);
            else
               l = Long.valueOf(l.longValue() + Long.valueOf(exacqValue).longValue());
            entry.put(exacq, l);
            
            l = entry.get(exmax);
            if (l == null)
               l = Long.valueOf(exmaxValue);
            else
            {
               Long t = Long.valueOf(exmaxValue);
               if (t.longValue() > l.longValue())
                  l = t;
            }
            entry.put(exmax, l);
            
            // MAX WAITERS
            l = entry.get(maxw);
            if (l == null)
               l = Long.valueOf(maxwValue);
            else
            {
               Long t = Long.valueOf(maxwValue);
               if (t.longValue() > l.longValue())
                  l = t;
            }
            entry.put(maxw, l);

            // BLOCK
            l = entry.get(blk);
            if (l == null)
               l = Long.valueOf(blkValue);
            else
               l = Long.valueOf(l.longValue() + Long.valueOf(blkValue).longValue());
            entry.put(blk, l);

            // SPINDELAY
            l = entry.get(spindelay);
            if (l == null)
               l = Long.valueOf(spindelayValue);
            else
               l = Long.valueOf(l.longValue() + Long.valueOf(spindelayValue).longValue());
            entry.put(spindelay, l);
            
            m.put(key, entry);
         }
         else if (data.startsWith("NAME"))
         {
            String s = data.substring(5);
            int index = s.indexOf("=");
            String key = s.substring(0, index);
            String value = s.substring(index + 1);
            names.put(key, value);
         }
            
         data = lnr.readLine();
      }

      fr.close();

      File weight = new File(profile + "-weight.txt");
      FileWriter weightFw = new FileWriter(weight);

      File exclusive = new File(profile + "-exclusive.txt");
      FileWriter exclusiveFw = new FileWriter(exclusive);
      
      File shared = new File(profile + "-shared.txt");
      FileWriter sharedFw = new FileWriter(shared);
      
      File block = new File(profile + "-block.txt");
      FileWriter blockFw = new FileWriter(block);
      
      File spin = new File(profile + "-spin.txt");
      FileWriter spinFw = new FileWriter(spin);
      
      for (Map.Entry<String, Map<String, Long>> e : m.entrySet())
      {
         String s = e.getKey();
         String realName = names.get(s);
         if (realName == null)
            realName = s;
         
         Map<String, Long> values = e.getValue();
         
         StringBuilder sb = new StringBuilder();
         
         sb.append("lwlock_stats;");
         
         Long max = values.get("maxw");

         for (long counter = 1; counter <= max.longValue(); counter++)
         {
            sb.append(counter).append(";");
         }
         
         sb.append(realName).append(";");
         sb.append(" ");

         Long shA = values.get("shacq");
         Long shM = values.get("shmax");
         Long exA = values.get("exacq");
         Long exM = values.get("exmax");
         Long b = values.get("blk");
         Long sd = values.get("spindelay");

         long w = shA.longValue() + (10 * exA.longValue());
            
         if (w > 0)
            write(sb.toString() + w, weightFw);
         if (exM.longValue() > 1)
            write(sb.toString() + exM, exclusiveFw);
         if (shM.longValue() > 1)
            write(sb.toString() + shM, sharedFw);
         if (b.longValue() > 0)
            write(sb.toString() + b, blockFw);
         if (sd.longValue() > 0)
            write(sb.toString() + sd, spinFw);
      }
      
      weightFw.flush();
      weightFw.close();
      
      exclusiveFw.flush();
      exclusiveFw.close();
      
      sharedFw.flush();
      sharedFw.close();
      
      blockFw.flush();
      blockFw.close();
      
      spinFw.flush();
      spinFw.close();
   }

   /**
    * Write a string to a file
    * @param s The string
    * @param fw The file
    */
   private static void write(String s, FileWriter fw) throws Exception
   {
      for (int i = 0; i < s.length(); i++)
      {
         fw.write((int)s.charAt(i));
      }
      fw.write('\n');
   }

   /**
    * Generate the flamegraphs
    * @param profile The name of the profile
    */
   private static void flamegraphs(String profile) throws Exception
   {
      flamegraph(profile + ": Weight", profile + "-weight");
      flamegraph(profile + ": Exclusive", profile + "-exclusive");
      flamegraph(profile + ": Shared", profile + "-shared");
      flamegraph(profile + ": Block", profile + "-block");
      flamegraph(profile + ": Spin", profile + "-spin");

      Files.deleteIfExists(Paths.get(profile + "-weight.txt"));
      Files.deleteIfExists(Paths.get(profile + "-exclusive.txt"));
      Files.deleteIfExists(Paths.get(profile + "-shared.txt"));
      Files.deleteIfExists(Paths.get(profile + "-block.txt"));
      Files.deleteIfExists(Paths.get(profile + "-spin.txt"));

   }

   /**
    * Generate a flamegraph
    * @param title The title
    * @param filename The file name
    */
   private static void flamegraph(String title, String filename) throws Exception
   {
      ProcessBuilder pb =
         new ProcessBuilder("perl", "flamegraph.pl", "--title=\'" + title + "\'", filename + ".txt");

      pb.directory(new File("."));

      Files.deleteIfExists(Paths.get(filename + ".svg"));
      Files.deleteIfExists(Paths.get(filename + ".error"));

      File output = new File(filename + ".svg");
      pb.redirectOutput(output);

      File error = new File(filename + ".error");
      pb.redirectError(error);

      Process p = pb.start();
      p.waitFor();

      if (Files.size(Paths.get(filename + ".error")) == 0)
         Files.deleteIfExists(Paths.get(filename + ".error"));
   }

   /**
    * Move the files to the report directory
    * @param profile The name of the profile
    */
   private static void moveFiles(String profile) throws Exception
   {
      File directory = new File("result");
      directory.mkdirs();

      Files.deleteIfExists(Paths.get("result/" + profile + ".log.gz"));
      compressGzipFile(profile + ".log", "result/" + profile + ".log.gz");
      Files.deleteIfExists(Paths.get(profile + ".log"));
      
      Files.move(Paths.get(profile + "-weight.svg"), Paths.get("result", profile + "-weight.svg"),
                 StandardCopyOption.REPLACE_EXISTING);
      Files.move(Paths.get(profile + "-exclusive.svg"), Paths.get("result", profile + "-exclusive.svg"),
                 StandardCopyOption.REPLACE_EXISTING);
      Files.move(Paths.get(profile + "-shared.svg"), Paths.get("result", profile + "-shared.svg"),
                 StandardCopyOption.REPLACE_EXISTING);
      Files.move(Paths.get(profile + "-block.svg"), Paths.get("result", profile + "-block.svg"),
                 StandardCopyOption.REPLACE_EXISTING);
      Files.move(Paths.get(profile + "-spin.svg"), Paths.get("result", profile + "-spin.svg"),
                 StandardCopyOption.REPLACE_EXISTING);
   }

   /**
    * Compress a file
    * @param file The source file
    * @param gzipFile The destination file
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
      // 1PC
      configurePostgreSQL();
      startPostgreSQL();
      initLogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      run1PCStandard();

      stopSlowPostgreSQL();
      
      copyLogfile("off-logged-1pc-standard");
      processLog("off-logged-1pc-standard");
      flamegraphs("off-logged-1pc-standard");
      moveFiles("off-logged-1pc-standard");

      // 1PCP
      configurePostgreSQL();
      startPostgreSQL();
      initLogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      run1PCPrepared();

      stopSlowPostgreSQL();
      
      copyLogfile("off-logged-1pc-prepared");
      processLog("off-logged-1pc-prepared");
      flamegraphs("off-logged-1pc-prepared");
      moveFiles("off-logged-1pc-prepared");

      // RO
      configurePostgreSQL();
      startPostgreSQL();
      initLogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      runReadOnly();

      stopSlowPostgreSQL();
      
      copyLogfile("off-logged-read-only");
      processLog("off-logged-read-only");
      flamegraphs("off-logged-read-only");
      moveFiles("off-logged-read-only");

      // 2PC
      if (RUN_TWOPC)
      {
         configurePostgreSQL();
         startPostgreSQL();
         initLogged();

         stopFastPostgreSQL();
         removeLog();
         startPostgreSQL();

         run2PCStandard();

         stopSlowPostgreSQL();
      
         copyLogfile("off-logged-2pc-standard");
         processLog("off-logged-2pc-standard");
         flamegraphs("off-logged-2pc-standard");
         moveFiles("off-logged-2pc-standard");
      }
   }
   
   /**
    * Off / Unlogged run
    */
   private static void offUnlogged() throws Exception
   {
      // 1PC
      configurePostgreSQL();
      startPostgreSQL();
      initUnlogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      run1PCStandard();

      stopSlowPostgreSQL();
      
      copyLogfile("off-unlogged-1pc-standard");
      processLog("off-unlogged-1pc-standard");
      flamegraphs("off-unlogged-1pc-standard");
      moveFiles("off-unlogged-1pc-standard");

      // 1PCP
      configurePostgreSQL();
      startPostgreSQL();
      initUnlogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      run1PCPrepared();

      stopSlowPostgreSQL();
      
      copyLogfile("off-unlogged-1pc-prepared");
      processLog("off-unlogged-1pc-prepared");
      flamegraphs("off-unlogged-1pc-prepared");
      moveFiles("off-unlogged-1pc-prepared");

      // RO
      configurePostgreSQL();
      startPostgreSQL();
      initUnlogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      runReadOnly();

      stopSlowPostgreSQL();
      
      copyLogfile("off-unlogged-read-only");
      processLog("off-unlogged-read-only");
      flamegraphs("off-unlogged-read-only");
      moveFiles("off-unlogged-read-only");

      // 2PC
      if (RUN_TWOPC)
      {
         configurePostgreSQL();
         startPostgreSQL();
         initUnlogged();

         stopFastPostgreSQL();
         removeLog();
         startPostgreSQL();
      
         run2PCStandard();

         stopSlowPostgreSQL();
      
         copyLogfile("off-unlogged-2pc-standard");
         processLog("off-unlogged-2pc-standard");
         flamegraphs("off-unlogged-2pc-standard");
         moveFiles("off-unlogged-2pc-standard");
      }
   }
   
   /**
    * On / Logged run
    */
   private static void onLogged() throws Exception
   {
      // 1PC
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initLogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      run1PCStandard();

      stopSlowPostgreSQL();
      
      copyLogfile("on-logged-1pc-standard");
      processLog("on-logged-1pc-standard");
      flamegraphs("on-logged-1pc-standard");
      moveFiles("on-logged-1pc-standard");

      // 1PCP
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initLogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      run1PCPrepared();

      stopSlowPostgreSQL();
      
      copyLogfile("on-logged-1pc-prepared");
      processLog("on-logged-1pc-prepared");
      flamegraphs("on-logged-1pc-prepared");
      moveFiles("on-logged-1pc-prepared");

      // RO
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initLogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      runReadOnly();

      stopSlowPostgreSQL();
      
      copyLogfile("on-logged-read-only");
      processLog("on-logged-read-only");
      flamegraphs("on-logged-read-only");
      moveFiles("on-logged-read-only");

      // 2PC
      if (RUN_TWOPC)
      {
         configurePostgreSQL();
         synchronousCommit();
         startPostgreSQL();
         initLogged();

         stopFastPostgreSQL();
         removeLog();
         startPostgreSQL();
      
         run2PCStandard();

         stopSlowPostgreSQL();
      
         copyLogfile("on-logged-2pc-standard");
         processLog("on-logged-2pc-standard");
         flamegraphs("on-logged-2pc-standard");
         moveFiles("on-logged-2pc-standard");
      }
   }

   /**
    * On / Unlogged run
    */
   private static void onUnlogged() throws Exception
   {
      // 1PC
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initUnlogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      run1PCStandard();

      stopSlowPostgreSQL();
      
      copyLogfile("on-unlogged-1pc-standard");
      processLog("on-unlogged-1pc-standard");
      flamegraphs("on-unlogged-1pc-standard");
      moveFiles("on-unlogged-1pc-standard");

      // 1PCP
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initUnlogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      run1PCPrepared();

      stopSlowPostgreSQL();
      
      copyLogfile("on-unlogged-1pc-prepared");
      processLog("on-unlogged-1pc-prepared");
      flamegraphs("on-unlogged-1pc-prepared");
      moveFiles("on-unlogged-1pc-prepared");

      // RO
      configurePostgreSQL();
      synchronousCommit();
      startPostgreSQL();
      initUnlogged();

      stopFastPostgreSQL();
      removeLog();
      startPostgreSQL();
      
      runReadOnly();

      stopSlowPostgreSQL();
      
      copyLogfile("on-unlogged-read-only");
      processLog("on-unlogged-read-only");
      flamegraphs("on-unlogged-read-only");
      moveFiles("on-unlogged-read-only");

      // 2PC
      if (RUN_TWOPC)
      {
         configurePostgreSQL();
         synchronousCommit();
         startPostgreSQL();
         initUnlogged();

         stopFastPostgreSQL();
         removeLog();
         startPostgreSQL();
      
         run2PCStandard();

         stopSlowPostgreSQL();
      
         copyLogfile("on-unlogged-2pc-standard");
         processLog("on-unlogged-2pc-standard");
         flamegraphs("on-unlogged-2pc-standard");
         moveFiles("on-unlogged-2pc-standard");
      }
   }

   /**
    * Read the configuration (lwlockrun.properties)
    */
   private static void readConfiguration() throws Exception
   {
      File f = new File("lwlockrun.properties");

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

         SLEEP_TIME = Long.valueOf(p.getProperty("SLEEP_TIME", Long.toString(SLEEP_TIME)));

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

         stopFastPostgreSQL();

         offLogged();
         offUnlogged();

         onLogged();
         onUnlogged();

         // Cleanup

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
