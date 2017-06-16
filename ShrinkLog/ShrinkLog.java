/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 Jesper Pedersen <jesper.pedersen@comcast.net>
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * ShrinkLog
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class ShrinkLog
{
   /** Default configuration */
   private static final String DEFAULT_CONFIGURATION = "shrinklog.properties";
   
   /** Log line type: EOF */
   private static final int EOF = -1;

   /** Log line type: UNKNOWN */
   private static final int UNKNOWN = 0;

   /** Log line type: PANIC */
   private static final int PANIC = 1;

   /** Log line type: FATAL */
   private static final int FATAL = 2;

   /** Log line type: ERROR */
   private static final int ERROR = 3;

   /** Log line type: WARNING */
   private static final int WARNING = 4;

   /** Log line type: INFO */
   private static final int INFO = 5;

   /** Log line type: DEBUG1 */
   private static final int DEBUG1 = 6;

   /** Log line type: DEBUG2 */
   private static final int DEBUG2 = 7;

   /** Log line type: DEBUG3 */
   private static final int DEBUG3 = 8;

   /** Log line type: DEBUG4 */
   private static final int DEBUG4 = 9;

   /** Log line type: DEBUG5 */
   private static final int DEBUG5 = 10;

   /** Log line type: STATEMENT */
   private static final int STATEMENT = 11;

   /** Log line type: DETAIL */
   private static final int DETAIL = 12;

   /** Log line type: LOG */
   private static final int LOG = 13;

   /** Log line type: NOTICE */
   private static final int NOTICE = 14;

   /** The configuration */
   private static Properties configuration;

   /** The file name */
   private static String filename;

   /** Max statements */
   private static int maxStatements;

   /** Skip */
   private static int skip;

   /** Statements:    Process  Number */
   private static Map<Integer, Integer> statements = new TreeMap<>();
   
   /** Transaction:   Process  Status */
   private static Map<Integer, Boolean> transactions = new TreeMap<>();

   /** Skip:          Process  Number */
   private static Map<Integer, Integer> skips = new TreeMap<>();

   /** Parse          Process */
   private static Set<Integer> parse = new TreeSet<>();

   /** Ended */
   private static Set<Integer> ended = new TreeSet<>();
   
   /** Parameters */
   private static Set<Integer> parameters = new TreeSet<>();
   
   /**
    * Open file
    * @param p The path of the file
    * @param s The data
    */
   private static BufferedWriter openFile(Path p) throws Exception
   {
      return Files.newBufferedWriter(p,
                                     StandardOpenOption.CREATE,
                                     StandardOpenOption.WRITE,
                                     StandardOpenOption.TRUNCATE_EXISTING);
   }

   /**
    * Close file
    * @param p The path of the file
    */
   private static void closeFile(BufferedWriter bw)
   {
      try
      {
         if (bw != null)
         {
            bw.flush();
            bw.close();
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   /**
    * Append data to a file
    * @param bw The file
    * @param s The data
    */
   private static void appendFile(BufferedWriter bw, String s) throws Exception
   {
      bw.write(s, 0, s.length());
      bw.newLine();
   }
   
   /**
    * Get the type of the log line
    * @param s The string
    * @return The type
    */
   private static int getLogLineType(String s)
   {
      if (s == null || "".equals(s))
         return EOF;

      int bracket1Start = s.indexOf("[");
      int bracket1End = s.indexOf("]");

      if (bracket1Start != -1)
      {
         int bracket2Start = s.indexOf("[", bracket1End + 1);
         int bracket2End = s.indexOf("]", bracket1End + 1);

         String type = s.substring(bracket2End + 2, s.indexOf(":", bracket2End + 2));

         if ("LOG".equals(type))
         {
            return LOG;
         }
         else if ("STATEMENT".equals(type))
         {
            return STATEMENT;
         }
         else if ("DETAIL".equals(type))
         {
            return DETAIL;
         }
         else if ("NOTICE".equals(type))
         {
            return NOTICE;
         }
         else if ("PANIC".equals(type))
         {
            return PANIC;
         }
         else if ("FATAL".equals(type))
         {
            return FATAL;
         }
         else if ("ERROR".equals(type))
         {
            return ERROR;
         }
         else if ("WARNING".equals(type))
         {
            return WARNING;
         }
         else if ("INFO".equals(type))
         {
            return INFO;
         }
         else if ("DEBUG1".equals(type))
         {
            return DEBUG1;
         }
         else if ("DEBUG2".equals(type))
         {
            return DEBUG2;
         }
         else if ("DEBUG3".equals(type))
         {
            return DEBUG3;
         }
         else if ("DEBUG4".equals(type))
         {
            return DEBUG4;
         }
         else if ("DEBUG5".equals(type))
         {
            return DEBUG5;
         }
         else
         {
            System.out.println("Unknown log line type for: " + s);
            System.exit(1);
         }
      }

      return UNKNOWN;
   }

   /**
    * Process the log
    * @param output The output file
    */
   private static void processLog(BufferedWriter output) throws Exception
   {
      FileReader fr = null;
      LineNumberReader lnr = null;
      String s = null;
      String str = null;
      LogEntry le = null;
      boolean include = true;
      try
      {
         fr = new FileReader(Paths.get(filename).toFile());
         lnr = new LineNumberReader(fr);
         s = lnr.readLine();

         while (s != null)
         {
            str = s;
            s = lnr.readLine();

            while (getLogLineType(s) == UNKNOWN)
            {
               str += " ";
               str += s.trim();
               s = lnr.readLine();
            }

            include = true;
            le = new LogEntry(str);

            if (!statements.containsKey(le.getProcessId()))
            {
               statements.put(le.getProcessId(), Integer.valueOf(0));
               transactions.put(le.getProcessId(), Boolean.FALSE);
               if (skip > 0)
                  skips.put(le.getProcessId(), Integer.valueOf(skip));
            }
            
            Integer skp = skips.get(le.getProcessId());
            if (skp != null)
            {
               include = false;

               if (le.isParse())
               {
                  parse.add(le.getProcessId());
                  if (transactions.get(le.getProcessId()) == null)
                  {
                     skp = Integer.valueOf(skp.intValue() - 1);
                     if (skp.intValue() < 0)
                     {
                        skp = null;
                        include = true;
                     }

                     skips.put(le.getProcessId(), skp);

                     if (le.getStatement().startsWith("BEGIN"))
                     {
                        transactions.put(le.getProcessId(), Boolean.TRUE);
                     }
                  }
               }
               else if (le.isBind())
               {
                  if (!parse.contains(le.getProcessId()))
                  {
                     if (transactions.get(le.getProcessId()) == null)
                     {
                        skp = Integer.valueOf(skp.intValue() - 1);
                        if (skp.intValue() < 0)
                        {
                           skp = null;
                           include = true;
                        }

                        skips.put(le.getProcessId(), skp);

                        if (le.getStatement().startsWith("BEGIN"))
                        {
                           transactions.put(le.getProcessId(), Boolean.TRUE);
                        }
                     }
                  }
               }
               else if (le.isExecute())
               {
                  parse.remove(le.getProcessId());

                  if (le.getStatement().startsWith("COMMIT"))
                  {
                     transactions.put(le.getProcessId(), null);
                  }
               }
            }

            if (include)
            {
               if (le.isParse())
               {
                  if (ended.contains(le.getProcessId()))
                  {
                     if (!Boolean.TRUE.equals(transactions.get(le.getProcessId())))
                        include = false;
                  }

                  if (include)
                  {
                     if (le.getStatement().startsWith("BEGIN"))
                     {
                        transactions.put(le.getProcessId(), Boolean.TRUE);
                     }
                  }
               }
               else if (le.isBind())
               {
                  if (ended.contains(le.getProcessId()))
                  {
                     if (!Boolean.TRUE.equals(transactions.get(le.getProcessId())))
                        include = false;
                  }

                  if (include)
                  {
                     if (le.getStatement().startsWith("BEGIN"))
                     {
                        transactions.put(le.getProcessId(), Boolean.TRUE);
                     }
                  }
               }
               else if (le.isExecute())
               {
                  if (!ended.contains(le.getProcessId()) ||
                      Boolean.TRUE.equals(transactions.get(le.getProcessId())))
                  {
                     Integer count = statements.get(le.getProcessId());
                     count = Integer.valueOf(count.intValue() + 1);
                     statements.put(le.getProcessId(), count);

                     if ((maxStatements != -1 && count.intValue() >= maxStatements) || ended.size() > 0)
                     {
                        ended.add(le.getProcessId());
                     }

                     if (le.getStatement().startsWith("COMMIT"))
                     {
                        transactions.put(le.getProcessId(), Boolean.FALSE);
                        if (ended.contains(le.getProcessId()))
                           parameters.add(le.getProcessId());
                     }
                     else if (le.getStatement().startsWith("ROLLBACK"))
                     {
                        transactions.put(le.getProcessId(), Boolean.FALSE);
                        if (ended.contains(le.getProcessId()))
                           parameters.add(le.getProcessId());
                     }
                  }
                  else
                  {
                     include = false;
                  }
               }
               else if (le.isParameters())
               {
                  if (ended.contains(le.getProcessId()) && !Boolean.TRUE.equals(transactions.get(le.getProcessId())))
                  {
                     if (parameters.contains(le.getProcessId()))
                     {
                        include = false;
                     }
                     else
                     {
                        parameters.add(le.getProcessId());
                     }
                  }
               }
               else
               {
                  include = false;
               }

               if (include)
                  appendFile(output, le.toString());
            }
         }
      }
      catch (Exception e)
      {
         System.out.println("Line: " + (lnr != null ? lnr.getLineNumber() : "?"));
         System.out.println("Data:");
         System.out.println(str);
         System.out.println("Line:");
         System.out.println(s);
         throw e;
      }
      finally
      {
         if (lnr != null)
            lnr.close();

         if (fr != null)
            fr.close();
      }
   }

   /**
    * Read the configuration (shrinklog.properties)
    * @param config The configuration
    */
   private static void readConfiguration(String config) throws Exception
   {
      File f = new File(config);
      configuration = new Properties();

      if (f.exists())
      {
         FileInputStream fis = null;
         try
         {
            fis = new FileInputStream(f);
            configuration.load(fis);
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
      }
   }

   /**
    * Main
    * @param args The arguments
    */
   public static void main(String[] args)
   {
      BufferedWriter output = null;
      try
      {
         if (args.length == 0 || args.length > 3)
         {
            System.out.println("Usage: ShrinkLog <log_file> [<max_statements>] [skip]");
            return;
         }

         String config = DEFAULT_CONFIGURATION;
         readConfiguration(config);

         filename = args[0];
         maxStatements = args.length > 1 ? Integer.valueOf(args[1]) : -1;
         skip = args.length > 2 ? Integer.valueOf(args[2]) : -1;
         output = openFile(Paths.get("output.log"));

         processLog(output);
         
         System.out.println("Clients: " + statements.size());
         int max = 0;
         for (Integer i : statements.values())
         {
            if (i.intValue() > max)
               max = i.intValue();
         }
         System.out.println("Max statements: " + max);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         closeFile(output);
      }
   }

   /**
    * Log entry
    */
   static class LogEntry
   {
      private int processId;
      private String timestamp;
      private int transactionId;
      private String fullStatement;
      private String statement;
      private boolean prepared;
      private boolean parse;
      private boolean bind;
      private boolean execute;
      private boolean parameters;
      
      LogEntry(String s)
      {
         int bracket1Start = s.indexOf("[");
         int bracket1End = s.indexOf("]");
         int bracket2Start = s.indexOf("[", bracket1End + 1);
         int bracket2End = s.indexOf("]", bracket1End + 1);

         this.processId = Integer.valueOf(s.substring(0, bracket1Start).trim());
         this.timestamp = s.substring(bracket1Start + 1, bracket1End);
         this.transactionId = Integer.valueOf(s.substring(bracket2Start + 1, bracket2End));
         this.fullStatement = s.substring(bracket2End + 2);

         this.statement = null;
         this.prepared = false;

         this.parse = isParse(this.fullStatement);
         this.bind = false;
         this.execute = false;
         this.parameters = false;

         if (!parse)
         {
            this.bind = isBind(this.fullStatement);
            if (!bind)
            {
               this.execute = isExecute(this.fullStatement);
               if (!execute)
               {
                  this.parameters = isParameters(this.fullStatement);
               }
            }
         }
      }

      int getProcessId()
      {
         return processId;
      }

      String getTimestamp()
      {
         return timestamp;
      }

      int getTransactionId()
      {
         return transactionId;
      }

      String getStatement()
      {
         return statement;
      }

      boolean isPrepared()
      {
         return prepared;
      }
      
      boolean isParse()
      {
         return parse;
      }
      
      boolean isBind()
      {
         return bind;
      }
      
      boolean isExecute()
      {
         return execute;
      }
      
      boolean isParameters()
      {
         return parameters;
      }
      
      /**
       * Is parse
       * @param line The log line
       * @return True if parse, otherwise false
       */
      private boolean isParse(String line)
      {
         int offset = line.indexOf("parse <");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replaceAll("\\$[0-9]*", "?");
            prepared = statement.indexOf("?") != -1;
            return true;
         }

         offset = line.indexOf("parse S");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replaceAll("\\$[0-9]*", "?");
            prepared = statement.indexOf("?") != -1;
            return true;
         }
      
         return false;
      }
      
      /**
       * Is bind
       * @param line The log line
       * @return True if bind, otherwise false
       */
      private boolean isBind(String line)
      {
         int offset = line.indexOf("bind <");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replaceAll("\\$[0-9]*", "?");
            prepared = statement.indexOf("?") != -1;
            return true;
         }

         offset = line.indexOf("bind S");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replaceAll("\\$[0-9]*", "?");
            prepared = statement.indexOf("?") != -1;
            return true;
         }
      
         return false;
      }
      
      /**
       * Is execute
       * @param line The log line
       * @return True if execute, otherwise false
       */
      private boolean isExecute(String line)
      {
         int offset = line.indexOf("execute <");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replaceAll("\\$[0-9]*", "?");
            prepared = statement.indexOf("?") != -1;
            return true;
         }
         
         offset = line.indexOf("execute S");
         if (offset != -1)
         {
            statement = line.substring(line.indexOf(":", offset) + 2);
            statement = statement.replaceAll("\\$[0-9]*", "?");
            prepared = statement.indexOf("?") != -1;
            return true;
         }
         
         return false;
      }
      
      /**
       * Is parameters
       * @param line The log line
       * @return True if execute, otherwise false
       */
      private boolean isParameters(String line)
      {
         int offset = line.indexOf("DETAIL:  parameters:");
         if (offset != -1)
         {
            statement = line.substring(offset + 21);
            statement = statement.replaceAll("\\$[0-9]*", "?");
            return true;
         }
         
         return false;
      }
      
      @Override
      public String toString()
      {
         return processId + " [" + timestamp + "] [" + transactionId + "] " + fullStatement;
      }
   }
}
