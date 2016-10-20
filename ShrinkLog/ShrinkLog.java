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
   
   /** The configuration */
   private static Properties configuration;

   /** The file name */
   private static String filename;

   /** Max statements */
   private static int maxStatements;

   /** Statements:    Process  Number */
   private static Map<Integer, Integer> statements = new TreeMap<>();
   
   /** Transaction:   Process  Status */
   private static Map<Integer, Boolean> transactions = new TreeMap<>();

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
    * Process the log
    * @param output The output file
    */
   private static void processLog(BufferedWriter output) throws Exception
   {
      FileReader fr = null;
      LineNumberReader lnr = null;
      String s = null;
      LogEntry le = null;
      boolean include = true;
      try
      {
         fr = new FileReader(Paths.get(filename).toFile());
         lnr = new LineNumberReader(fr);

         while ((s = lnr.readLine()) != null)
         {
            include = true;
            le = new LogEntry(s);

            if (!statements.containsKey(le.getProcessId()))
            {
               statements.put(le.getProcessId(), Integer.valueOf(0));
               transactions.put(le.getProcessId(), Boolean.FALSE);
            }
            
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

            if (include)
               appendFile(output, le.toString());
         }
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
    * Read the configuration (replay.properties)
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
         if (args.length != 1 && args.length != 2)
         {
            System.out.println("Usage: ShrinkLog <log_file> [<max_statements>]");
            return;
         }

         String config = DEFAULT_CONFIGURATION;
         readConfiguration(config);

         filename = args[0];
         maxStatements = args.length > 1 ? Integer.valueOf(args[1]) : -1;
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
