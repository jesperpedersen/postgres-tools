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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

/**
 * Dataflow analysis
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class Dataflow
{
   /** Raw data:      Process  Log */
   private static Map<Integer, List<String>> rawData = new TreeMap<>();

   /** Data:          Process  LogEntry */
   private static Map<Integer, List<LogEntry>> data = new TreeMap<>();

   /** The file name */
   private static String filename;

   /** Affected processes */
   private static TreeSet<Integer> processes = new TreeSet<>();

   /** Position */
   private static int position = -1;

   /** UPDATE:        Key     Value */
   private static Map<String, String> updateValues = new TreeMap<>();

   /** Aliases:       Alias   Name */
   private static Map<String, String> aliases = new TreeMap<>();

   /** Current table name */
   private static String currentTableName = null;

   /**
    * Write data to a file
    * @param p The path of the file
    * @param l The data
    */
   private static void writeFile(Path p, List<String> l) throws Exception
   {
      BufferedWriter bw = Files.newBufferedWriter(p,
                                                  StandardOpenOption.CREATE,
                                                  StandardOpenOption.WRITE,
                                                  StandardOpenOption.TRUNCATE_EXISTING);
      for (String s : l)
      {
         bw.write(s, 0, s.length());
         bw.newLine();
      }

      bw.flush();
      bw.close();
   }
   
   /**
    * Process the log
    */
   private static void processLog() throws Exception
   {
      List<String> l = Files.readAllLines(Paths.get(filename));

      for (int i = 0; i < l.size(); i++)
      {
         String s = l.get(i);
         LogEntry le = new LogEntry(s);

         // Raw data insert
         List<String> ls = rawData.get(le.getProcessId());
         if (ls == null)
            ls = new ArrayList<>();
         ls.add(s);
         rawData.put(le.getProcessId(), ls);

         // Data insert
         List<LogEntry> lle = data.get(le.getProcessId());
         if (lle == null)
            lle = new ArrayList<>();
         lle.add(le);
         data.put(le.getProcessId(), lle);
      }

      for (Integer proc : rawData.keySet())
      {
         List<String> write = rawData.get(proc);
         writeFile(Paths.get("report", proc + ".log"), write);
      }      
   }

   /**
    * Do analysis
    */
   private static void doAnalysis() throws Exception
   {
      for (Integer proc : data.keySet())
      {
         List<LogEntry> lle = data.get(proc);
         List<DataEntry> l = new ArrayList<>();

         for (int i = 0; i < lle.size(); i++)
         {
            LogEntry le = lle.get(i);
            if (le.isExecute())
            {
               DataEntry de = new DataEntry();
               de.setPrepared(le.isPrepared());
               de.setStatement(le.getStatement());

               if (i < lle.size() - 1)
               {
                  LogEntry next = lle.get(i + 1);
                  if (next.isParameters())
                  {
                     List<String> parameters = new ArrayList<>();
                     StringTokenizer st = new StringTokenizer(next.getStatement(), "?");

                     while (st.hasMoreTokens())
                     {
                        String token = st.nextToken();
                        int start = token.indexOf("'");
                        int end = token.lastIndexOf("'");
                        String value = null;
                        if (start != -1 && end != -1)
                        {
                           value = token.substring(start + 1, end);
                        }
                        parameters.add(value);
                     }
                     
                     de.setParameters(parameters);
                     i++;
                  }
               }
               
               l.add(de);
            }
         }

         processInteraction(proc, l);
      } 
   }

   /**
    * Process interaction
    * @param proc The process
    * @param dl The data
    */
   private static void processInteraction(Integer proc, List<DataEntry> dl) throws Exception
   {
      boolean include = false;
      List<String> stmts = new ArrayList<>();
      
      for (DataEntry de : dl)
      {
         try
         {
            include = false;
            
            if (de.getStatement().toUpperCase().startsWith("BEGIN"))
            {
               updateValues.clear();
            }
            else if (de.getStatement().toUpperCase().startsWith("UPDATE"))
            {
               net.sf.jsqlparser.statement.Statement s = CCJSqlParserUtil.parse(de.getStatement());
               Update update = (Update)s;

               final Map<String, String> set = new TreeMap<>();
               final Map<String, String> where = new TreeMap<>();
               position = 0;

               for (Table table : update.getTables())
               {
                  currentTableName = table.getName();
               }

               StringBuilder buffer = new StringBuilder();
               ExpressionDeParser expressionDeParser = new ExpressionDeParser()
               {
                  private Column currentColumn = null;
                  private boolean isSET = false;
            
                  @Override
                  public void visit(Column column)
                  {
                     currentColumn = column;
                     
                     isSET = false;
                     for (Column c : update.getColumns())
                     {
                        if (c.getColumnName().equals(column.getColumnName()))
                           isSET = true;
                     }

                     this.getBuffer().append(column);
                  }

                  @Override
                  public void visit(JdbcParameter jdbcParameter)
                  {
                     String value = de.getParameters().get(position);

                     if (isSET)
                     {
                        set.put(currentColumn.getColumnName(), value);
                     }
                     else
                     {
                        where.put(currentColumn.getColumnName(), value);
                     }

                     position++;

                     this.getBuffer().append(value);
                  }
               };

               SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, buffer)
               {
                  @Override
                  public void visit(Table table)
                  {
                     currentTableName = table.getName();
               
                     if (table.getAlias() != null && !table.getAlias().getName().equals(""))
                        aliases.put(table.getAlias().getName(), table.getName());

                     this.getBuffer().append(table);
                  }
               };
               expressionDeParser.setSelectVisitor(selectDeParser);
               expressionDeParser.setBuffer(buffer);

               net.sf.jsqlparser.util.deparser.UpdateDeParser updateDeParser =
                  new net.sf.jsqlparser.util.deparser.UpdateDeParser(expressionDeParser, selectDeParser, buffer);

               net.sf.jsqlparser.util.deparser.StatementDeParser statementDeParser =
                  new net.sf.jsqlparser.util.deparser.StatementDeParser(buffer)
               {
                  @Override
                  public void visit(Update update)
                  {
                     updateDeParser.deParse(update);
                  }
               };
         
               update.accept(statementDeParser);

               // Rule 1: Make sure that SET columns doesn't contain same value as WHERE columns
               if (!set.isEmpty())
               {
                  for (Map.Entry<String,String> we : where.entrySet())
                  {
                     if (set.containsKey(we.getKey()))
                     {
                        if (set.get(we.getKey()).equals(we.getValue()))
                        {
                           stmts.add(buffer.toString());
                           include = true;
                           break;
                        }
                     }
                  }
               }

               // Rule 2: Make sure that SET columns are unique
               if (!include && !set.isEmpty())
               {
                  String key = currentTableName + "@" + where.toString();
                  String val = updateValues.get(key);

                  if (val != null && val.equals(set.toString()))
                  {
                     stmts.add(buffer.toString());
                     include = true;
                  }

                  updateValues.put(key, set.toString());
               }
            }

            if (include)
            {
               processes.add(proc);

               List<String> l = new ArrayList<>();
               l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
               l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
               l.add("");
               l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
               l.add("<head>");
               l.add("  <title>Dataflow: " + proc + "</title>");
               l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
               l.add("</head>");
               l.add("<body>");
               l.add("<h1>Process: " + proc + "</h1>");
               l.add("");
               
               l.add("<pre>");
               for (String s : stmts)
               {
                  l.add(s);
               }
               l.add("</pre>");

               l.add("<p>");
               l.add("<a href=\"index.html\">Back</a>");
               
               l.add("</body>");
               l.add("</html>");
            
               writeFile(Paths.get("report", proc + ".html"), l);
            }
         }
         catch (Exception e)
         {
            System.out.println(de);
            throw e;
         }
      }
   }
   
   /**
    * Write index.html
    */
   private static void writeIndex() throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>Dataflow analysis</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>Dataflow analysis</h1>");
      l.add("");

      if (!processes.isEmpty())
      {
         l.add("Issue reports");
         l.add("<ul>");
         for (Integer proc : processes)
         {
            l.add("<li><a href=\"" + proc + ".html\">" + proc + "</a></li>");
         }
         l.add("</ul>");
      }
      else
      {
         l.add("No issues detected");
      }

      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "index.html"), l);
   }

   /**
    * Main
    * @param args The arguments
    */
   public static void main(String[] args)
   {
      try
      {
         if (args.length != 1)
         {
            System.out.println("Usage: Dataflow <log_file>");
            return;
         }

         filename = args[0];

         File directory = new File("report");
         directory.mkdirs();

         processLog();
         doAnalysis();

         writeIndex();
      }
      catch (Exception e)
      {
         e.printStackTrace();
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
         this.bind = isBind(this.fullStatement);
         this.execute = isExecute(this.fullStatement);
         this.parameters = isParameters(this.fullStatement);
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

   /**
    * Data entry
    */
   static class DataEntry
   {
      private boolean prepared;
      private String statement;
      private List<String> parameters;
      
      DataEntry()
      {
         prepared = false;
         statement = null;
         parameters = null;
      }

      /**
       * Is prepared
       * @return The value
       */
      boolean isPrepared()
      {
         return prepared;
      }
      
      /**
       * Set prepared
       * @param v The value
       */
      void setPrepared(boolean v)
      {
         prepared = v;
      }
      
      /**
       * Get statement
       * @return The value
       */
      String getStatement()
      {
         return statement;
      }
      
      /**
       * Set statement
       * @param v The value
       */
      void setStatement(String v)
      {
         statement = v;
      }
      
      /**
       * Get parameters
       * @return The values
       */
      List<String> getParameters()
      {
         return parameters;
      }
      
      /**
       * Set parameters
       * @param v The value
       */
      void setParameters(List<String> v)
      {
         parameters = v;
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public String toString()
      {
         return "DataEntry{statement=" + statement + ", parameters=" + parameters + "}";
      }
   }
}
