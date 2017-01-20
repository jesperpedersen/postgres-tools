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
import java.io.FilenameFilter;
import java.io.LineNumberReader;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import javax.xml.bind.DatatypeConverter;

import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

/**
 * Replay
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class Replay
{
   /** Default configuration */
   private static final String DEFAULT_CONFIGURATION = "replay.properties";
   
   /** The configuration */
   private static Properties configuration;

   /** Data:          Process  LogEntry */
   private static Map<Integer, List<LogEntry>> data = new TreeMap<>();

   /** The file name */
   private static String filename;
   
   /** The profile name */
   private static String profilename;

   /** Column types   Table       Column  Type */
   private static Map<String, Map<String, Integer>> columnTypes = new TreeMap<>();
   
   /** Aliases:       Alias   Name */
   private static Map<String, String> aliases = new TreeMap<>();

   /** Current table name */
   private static String currentTableName = null;

   /** Iterate through ResultSet */
   private static boolean resultSet = false;

   /** Parallel execution */
   private static boolean parallelExecution = true;

   /** XA */
   private static boolean xa = false;

   /** XADataSource */
   private static XADataSource xaDataSource = null;

   /** NG driver */
   private static boolean ngDriver = false;

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
      FileReader fr = null;
      LineNumberReader lnr = null;
      String s = null;
      LogEntry le = null;
      boolean execute = false;
      try
      {
         fr = new FileReader(Paths.get(filename).toFile());
         lnr = new LineNumberReader(fr);

         while ((s = lnr.readLine()) != null)
         {
            le = new LogEntry(s);

            if (le.isParse() || le.isBind())
            {
               execute = false;
            }
            else if (le.isExecute() || (execute && le.isParameters()))
            {
               execute = true;

               List<LogEntry> lle = data.get(le.getProcessId());
               if (lle == null)
                  lle = new ArrayList<>();
               lle.add(le);
               data.put(le.getProcessId(), lle);
            }
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
    * Create interaction
    * @param c The connection
    */
   private static void createInteraction(Connection c) throws Exception
   {
      for (Integer proc : data.keySet())
      {
         List<LogEntry> lle = data.get(proc);
         List<String> l = new ArrayList<>();

         for (int i = 0; i < lle.size(); i++)
         {
            LogEntry le = lle.get(i);
            if (le.isExecute())
            {
               DataEntry de = new DataEntry();
               de.setPrepared(le.isPrepared());

               String stmt = le.getStatement();
               if (stmt.startsWith("PREPARE TRANSACTION"))
               {
                  continue;
               }
               else if (stmt.startsWith("COMMIT PREPARED"))
               {
                  stmt = "COMMIT";
               }
               else if (stmt.startsWith("ROLLBACK PREPARED"))
               {
                  stmt = "ROLLBACK";
               }
               de.setStatement(stmt);

               if (i < lle.size() - 1)
               {
                  LogEntry next = lle.get(i + 1);
                  if (next.isParameters())
                  {
                     List<String> parameters = new ArrayList<>();
                     StringTokenizer st = new StringTokenizer(next.getStatement(), "?");
                     List<Integer> types = getParameterTypes(c, le.getStatement(), st.countTokens());

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
                     
                     de.setTypes(types);
                     de.setParameters(parameters);
                     i++;
                  }
               }

               l.addAll(de.getData());
            }
         }

         if (l.size() > 0)
            writeFile(Paths.get(profilename, proc + ".cli"), l);
      }      
   }

   /**
    * Get the parameter types of a query
    * @param c The connection
    * @param query The query
    * @param num The number of required parameter types
    * @return The types
    */
   private static List<Integer> getParameterTypes(Connection c, String query, int num) throws Exception
   {
      List<Integer> result = new ArrayList<>();

      net.sf.jsqlparser.statement.Statement s = CCJSqlParserUtil.parse(query);

      if (s instanceof Select)
      {
         Select select = (Select)s;
         
         StringBuilder buffer = new StringBuilder();
         ExpressionDeParser expressionDeParser = new ExpressionDeParser()
         {
            private Column currentColumn = null;
            
            @Override
            public void visit(Column column)
            {
               currentColumn = column;
            }

            @Override
            public void visit(JdbcParameter jdbcParameter)
            {
               String table = currentColumn.getTable().getName();
               String column = currentColumn.getColumnName();

               if (table == null)
                  table = currentTableName;
               
               if (aliases.containsKey(table))
                  table = aliases.get(table);
               
               try
               {
                  result.add(getType(c, table, column));
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         };

         SelectDeParser deparser = new SelectDeParser(expressionDeParser, buffer)
         {
            @Override
            public void visit(Table table)
            {
               currentTableName = table.getName();
               
               if (table.getAlias() != null && !table.getAlias().getName().equals(""))
                  aliases.put(table.getAlias().getName(), table.getName());
            }
         };
         expressionDeParser.setSelectVisitor(deparser);
         expressionDeParser.setBuffer(buffer);

         select.getSelectBody().accept(deparser);
      }
      else if (s instanceof Update)
      {
         Update update = (Update)s;

         for (Table table : update.getTables())
         {
            currentTableName = table.getName();
         }

         StringBuilder buffer = new StringBuilder();
         ExpressionDeParser expressionDeParser = new ExpressionDeParser()
         {
            private Column currentColumn = null;
            
            @Override
            public void visit(Column column)
            {
               currentColumn = column;
            }

            @Override
            public void visit(JdbcParameter jdbcParameter)
            {
               String table = currentColumn.getTable().getName();
               String column = currentColumn.getColumnName();

               if (table == null)
                  table = currentTableName;
               
               if (aliases.containsKey(table))
                  table = aliases.get(table);
               
               try
               {
                  result.add(getType(c, table, column));
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
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
      }
      else if (s instanceof Delete)
      {
         Delete delete = (Delete)s;
         currentTableName = delete.getTable().getName();

         StringBuilder buffer = new StringBuilder();
         ExpressionDeParser expressionDeParser = new ExpressionDeParser()
         {
            private Column currentColumn = null;
            
            @Override
            public void visit(Column column)
            {
               currentColumn = column;
            }

            @Override
            public void visit(JdbcParameter jdbcParameter)
            {
               String table = currentColumn.getTable().getName();
               String column = currentColumn.getColumnName();

               if (table == null)
                  table = currentTableName;
               
               if (aliases.containsKey(table))
                  table = aliases.get(table);
               
               try
               {
                  result.add(getType(c, table, column));
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         };
         expressionDeParser.setBuffer(buffer);

         net.sf.jsqlparser.util.deparser.DeleteDeParser deleteDeParser =
            new net.sf.jsqlparser.util.deparser.DeleteDeParser(expressionDeParser, buffer);

         net.sf.jsqlparser.util.deparser.StatementDeParser statementDeParser =
            new net.sf.jsqlparser.util.deparser.StatementDeParser(buffer)
         {
            @Override
            public void visit(Delete delete)
            {
               deleteDeParser.deParse(delete);
            }
         };
         
         delete.accept(statementDeParser);
      }
      else if (s instanceof Insert)
      {
         Insert insert = (Insert)s;
         currentTableName = insert.getTable().getName();

         for (Column currentColumn : insert.getColumns())
         {
            String table = currentColumn.getTable().getName();
            String column = currentColumn.getColumnName();

            if (table == null)
               table = currentTableName;
               
            if (aliases.containsKey(table))
               table = aliases.get(table);
               
            try
            {
               result.add(getType(c, table, column));
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      }

      if (result.size() != num)
         System.out.println("Incomplete data for query: " + query);
      
      return result;
   }

   /**
    * Get the type of a column
    * @param c The connection
    * @param table The table
    * @param column The column
    * @return The type
    */
   private static int getType(Connection c, String table, String column) throws Exception
   {
      Map<String, Integer> tableData = columnTypes.get(table.toLowerCase(Locale.US));

      if (tableData == null)
      {
         tableData = new TreeMap<>();
         ResultSet rs = null;
         try
         {
            DatabaseMetaData dmd = c.getMetaData();
            rs = dmd.getColumns(null, null, table.toLowerCase(Locale.US), "");
            while (rs.next())
            {
               String columnName = rs.getString("COLUMN_NAME");
               int dataType = rs.getInt("DATA_TYPE");
               tableData.put(columnName.toLowerCase(Locale.US), dataType);
            }
            
            columnTypes.put(table.toLowerCase(Locale.US), tableData);
         }
         finally
         {
            if (rs != null)
            {
               try
               {
                  rs.close();
               }
               catch (Exception e)
               {
                  // Ignore
               }
            }
         }
      }
      
      return tableData.get(column.toLowerCase(Locale.US));
   }

   /**
    * Execute clients
    * @param url The database url
    * @param user The database user
    * @param password The password
    */
   private static void executeClients() throws Exception
   {
      File directory = new File(profilename);
      File[] clientData = directory.listFiles(new FilenameFilter()
                                              { 
                                                 public boolean accept(File directory, String filename)
                                                 {
                                                    return filename.endsWith(".cli");
                                                 }
                                              });

      List<Client> clients = new ArrayList<>(clientData.length);
      CountDownLatch clientReady = new CountDownLatch(clientData.length);
      CountDownLatch clientRun = new CountDownLatch(1);
      CountDownLatch clientDone = new CountDownLatch(clientData.length);
      int statements = 0;
      ExecutorService es = null;
      long start = 0;
      long end = 0;
      
      for (File f : clientData)
      {
         List<String> l = Files.readAllLines(f.toPath());
         List<DataEntry> interaction = new ArrayList<>();

         int i = 0;
         while (i < l.size())
         {
            String prepared = l.get(i++);
            String statement = l.get(i++);
            String types = l.get(i++);
            String parameters = l.get(i++);
            interaction.add(new DataEntry(prepared, statement, types, parameters));
            statements++;
         }

         clients.add(new Client(Integer.valueOf(f.getName().substring(0, f.getName().indexOf("."))),
                                interaction,
                                clientReady, clientRun, clientDone));
      }

      if (parallelExecution)
      {
         es = Executors.newFixedThreadPool(clients.size());

         for (Client cli : clients)
         {
            es.submit(cli);
         }

         clientReady.await();

         start = System.currentTimeMillis();
         clientRun.countDown();
         clientDone.await();
         end = System.currentTimeMillis();
      }
      else
      {
         clientRun.countDown();

         start = System.currentTimeMillis();
         for (Client cli : clients)
         {
            cli.run();
         }
         end = System.currentTimeMillis();
      }

      System.out.println("Clock: " + (end - start) + "ms");
      System.out.println("  Number of clients: " + clients.size());
      System.out.println("  Statements: " + statements);
      for (Client cli : clients)
      {
         System.out.println("  " + cli.getId() + ": " + cli.getRunTime() + "/" + cli.getConnectionTime());
      }
      
      if (es != null)
         es.shutdown();

      writeCSV(end - start, clients);
   }

   /**
    * Write CSV file
    * @param clock The clock time
    * @param clients The clients
    */
   private static void writeCSV(long clock, List<Client> clients) throws Exception
   {
      List<String> l = new ArrayList<>();

      l.add("Clock," + clock + "," + clock);

      for (Client cli : clients)
      {
         l.add(cli.getId() + "," + cli.getRunTime() + "," + cli.getConnectionTime());
      }

      writeFile(Paths.get(profilename, "result.csv"), l);
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
    * Get a XAConnection
    * @return The connection
    */
   private static synchronized XAConnection getXAConnection() throws Exception
   {
      if (xaDataSource == null)
      {
         String host = configuration.getProperty("host", "localhost");
         int port = Integer.valueOf(configuration.getProperty("port", "5432"));
         String database = configuration.getProperty("database");

         if (!ngDriver)
         {
            Class<?> clz = Class.forName("org.postgresql.xa.PGXADataSource");
            Object obj = clz.newInstance();

            Method mHost = clz.getMethod("setServerName", String.class);
            mHost.invoke(obj, host);

            Method mPort = clz.getMethod("setPortNumber", int.class);
            mPort.invoke(obj, port);

            Method mDatabase = clz.getMethod("setDatabaseName", String.class);
            mDatabase.invoke(obj, database);

            xaDataSource = (XADataSource)obj;
         }
         else
         {
            Class<?> clz = Class.forName("com.impossibl.postgres.jdbc.xa.PGXADataSource");
            Object obj = clz.newInstance();

            Method mHost = clz.getMethod("setHost", String.class);
            mHost.invoke(obj, host);

            Method mPort = clz.getMethod("setPort", int.class);
            mPort.invoke(obj, port);

            Method mDatabase = clz.getMethod("setDatabase", String.class);
            mDatabase.invoke(obj, database);

            xaDataSource = (XADataSource)obj;
         }
      }

      return xaDataSource.getXAConnection(configuration.getProperty("user"),
                                          configuration.getProperty("password"));
   }

   /**
    * Main
    * @param args The arguments
    */
   public static void main(String[] args)
   {
      Connection c = null;
      try
      {
         if (args.length != 1 && args.length != 2)
         {
            System.out.println("Usage: Replay -i <log_file>            (init)");
            System.out.println("       Replay [-r] [-s] [-x] <profile> (run)");
            return;
         }

         String config = DEFAULT_CONFIGURATION;
         readConfiguration(config);

         String url = null;
         
         if (configuration.getProperty("url") == null)
         {
            String database = configuration.getProperty("database");
            if (database == null)
            {
               System.out.println("database not defined.");
               return;
            }

            url = "jdbc:postgresql://" + configuration.getProperty("host", "localhost") +
               ":" + configuration.getProperty("port", "5432") + "/" + database;
         }
         else
         {
            url = configuration.getProperty("url");

            int doubleSlash = url.indexOf("//");
            int slash = url.indexOf("/", doubleSlash + 2);
            int col = url.indexOf(":", doubleSlash);
            String host = url.substring(doubleSlash + 2, col != -1 ? col : slash);
            String port = "5432";
            String database = url.substring(slash + 1);

            if (col != -1)
               port = url.substring(col + 1, slash);

            if (url.indexOf(":pgsql:") != -1)
               ngDriver = true;

            configuration.setProperty("host", host);
            configuration.setProperty("port", port);
            configuration.setProperty("database", database);
         }

         String user = configuration.getProperty("user");
         if (user == null)
         {
            System.out.println("user not defined.");
            return;
         }

         String password = configuration.getProperty("password");
         if (password == null)
         {
            System.out.println("password not defined.");
            return;
         }

         if ("-i".equals(args[0]))
         {
            filename = args[1];

            profilename = filename.substring(0, filename.lastIndexOf("."));
            
            c = DriverManager.getConnection(url, user, password);

            File directory = new File(profilename);
            directory.mkdirs();

            processLog();
            createInteraction(c);
         }
         else
         {
            for (int parameter = 0; parameter < args.length - 1; parameter++)
            {
               if ("-r".equals(args[parameter]))
               {
                  resultSet = true;
               }
               else if ("-s".equals(args[parameter]))
               {
                  parallelExecution = false;
               }
               else if ("-x".equals(args[parameter]))
               {
                  xa = true;
               }
            }

            profilename = args[args.length - 1];

            executeClients();
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (c != null)
         {
            try
            {
               c.close();
            }
            catch (Exception e)
            {
               // Nothing to do
            }
         }
      }
   }

   /**
    * Client
    */
   static class Client implements Runnable
   {
      /** Identifier */
      private int identifier;

      /** Interaction data */
      private List<DataEntry> interaction;

      /** Client ready */
      private CountDownLatch clientReady;

      /** Client run */
      private CountDownLatch clientRun;

      /** Client done */
      private CountDownLatch clientDone;

      /** Success */
      private boolean success;

      /** Before connection */
      private long beforeConnection;

      /** After connection */
      private long afterConnection;

      /** Before run */
      private long beforeRun;

      /** After run */
      private long afterRun;

      /**
       * Constructor
       */
      Client(int identifier, List<DataEntry> interaction,
             CountDownLatch clientReady, CountDownLatch clientRun, CountDownLatch clientDone)
      {
         this.identifier = identifier;
         this.interaction = interaction;
         this.clientReady = clientReady;
         this.clientRun = clientRun;
         this.clientDone = clientDone;
         this.success = false;
      }

      /**
       * Get id
       * @return The value
       */
      int getId()
      {
         return identifier;
      }

      /**
       * Is success
       * @return The value
       */
      boolean isSuccess()
      {
         return success;
      }

      /**
       * Get the connection time
       * @return The value
       */
      long getConnectionTime()
      {
         return afterConnection - beforeConnection;
      }

      /**
       * Get the run time
       * @return The value
       */
      long getRunTime()
      {
         return afterRun - beforeRun;
      }
      
      /**
       * Do the interaction
       */
      public void run()
      {
         beforeConnection = System.currentTimeMillis();
         XAConnection xc = null;
         Xid xid = null;
         Connection c = null;
         DataEntry de = null;
         try
         {
            try
            {
               if (xa)
               {
                  xc = getXAConnection();
                  c = xc.getConnection();
                  xid = new XidImpl(identifier);
               }
               else
               {
                  String url = null;
                  if (!ngDriver)
                  {
                     url = "jdbc:postgresql://" + configuration.getProperty("host", "localhost") + ":" +
                        configuration.getProperty("port", "5432") + "/" + configuration.getProperty("database");
                  }
                  else
                  {
                     url = "jdbc:pgsql://" + configuration.getProperty("host", "localhost") + ":" +
                        configuration.getProperty("port", "5432") + "/" + configuration.getProperty("database");
                  }

                  c = DriverManager.getConnection(url, configuration.getProperty("user"),
                                                  configuration.getProperty("password"));
               }
            }
            catch (Exception ce)
            {
               clientReady.countDown();
               throw ce;
            }

            clientReady.countDown();
            clientRun.await();
            beforeRun = System.currentTimeMillis();

            for (int counter = 0; counter < interaction.size(); counter++)
            {
               de = interaction.get(counter);
               if ("BEGIN".equals(de.getStatement()))
               {
                  c.setAutoCommit(false);
                  if (xa)
                  {
                     xc.getXAResource().start(xid, XAResource.TMNOFLAGS);
                  }
               }
               else if ("ROLLBACK".equals(de.getStatement()))
               {
                  if (xa)
                  {
                     xc.getXAResource().end(xid, XAResource.TMFAIL);
                     xc.getXAResource().prepare(xid);
                     xc.getXAResource().rollback(xid);
                  }
                  else
                  {
                     c.rollback();
                  }
                  c.setAutoCommit(true);
               }
               else if ("COMMIT".equals(de.getStatement()))
               {
                  if (xa)
                  {
                     xc.getXAResource().end(xid, XAResource.TMSUCCESS);
                     xc.getXAResource().prepare(xid);
                     xc.getXAResource().commit(xid, false);
                  }
                  else
                  {
                     c.commit();
                  }
                  c.setAutoCommit(true);
               }
               else
               {
                  if (!de.isPrepared())
                  {
                     Statement stmt = c.createStatement();
                     if (stmt.execute(de.getStatement()) && resultSet)
                     {
                        ResultSet rs = stmt.getResultSet();
                        while (rs.next())
                        {
                           // Just advance
                        }
                        rs.close();
                     }
                     stmt.close();
                  }
                  else
                  {
                     PreparedStatement ps = c.prepareStatement(de.getStatement());

                     List<Integer> types = de.getTypes();
                     List<String> parameters = de.getParameters();

                     if (types != null)
                     {
                        for (int i = 0; i < types.size(); i++)
                        {
                           int type = types.get(i);
                           String value = parameters.get(i);

                           if ("null".equals(value))
                           {
                              ps.setObject(i + 1, null);
                           }
                           else
                           {
                              switch (type)
                              {
                                 case Types.BINARY:
                                    ps.setBytes(i + 1, DatatypeConverter.parseHexBinary(value.substring(2)));
                                    break;
                                 case Types.BIT:
                                    ps.setBoolean(i + 1, Boolean.valueOf(value));
                                    break;
                                 case Types.BIGINT:
                                    ps.setLong(i + 1, Long.valueOf(value));
                                    break;
                                 case Types.BOOLEAN:
                                    ps.setBoolean(i + 1, Boolean.valueOf(value));
                                    break;
                                 case Types.CHAR:
                                    ps.setString(i + 1, value);
                                    break;
                                 case Types.DATE:
                                    ps.setDate(i + 1, java.sql.Date.valueOf(value));
                                    break;
                                 case Types.DECIMAL:
                                    ps.setDouble(i + 1, Double.valueOf(value));
                                    break;
                                 case Types.DOUBLE:
                                    ps.setDouble(i + 1, Double.valueOf(value));
                                    break;
                                 case Types.FLOAT:
                                    ps.setFloat(i + 1, Float.valueOf(value));
                                    break;
                                 case Types.INTEGER:
                                    ps.setInt(i + 1, Integer.valueOf(value));
                                    break;
                                 case Types.LONGVARBINARY:
                                    ps.setBytes(i + 1, DatatypeConverter.parseHexBinary(value.substring(2)));
                                    break;
                                 case Types.LONGVARCHAR:
                                    ps.setString(i + 1, value);
                                    break;
                                 case Types.NUMERIC:
                                    ps.setDouble(i + 1, Double.valueOf(value));
                                    break;
                                 case Types.REAL:
                                    ps.setFloat(i + 1, Float.valueOf(value));
                                    break;
                                 case Types.SMALLINT:
                                    ps.setShort(i + 1, Short.valueOf(value));
                                    break;
                                 case Types.TIME:
                                 case Types.TIME_WITH_TIMEZONE:
                                    ps.setTime(i + 1, java.sql.Time.valueOf(value));
                                    break;
                                 case Types.TIMESTAMP:
                                 case Types.TIMESTAMP_WITH_TIMEZONE:
                                    ps.setTimestamp(i + 1, java.sql.Timestamp.valueOf(value));
                                    break;
                                 case Types.TINYINT:
                                    ps.setShort(i + 1, Short.valueOf(value));
                                    break;
                                 case Types.VARBINARY:
                                    ps.setBytes(i + 1, DatatypeConverter.parseHexBinary(value.substring(2)));
                                    break;
                                 case Types.VARCHAR:
                                    ps.setString(i + 1, value);
                                    break;
                                 default:
                                    System.out.println("Unsupported value: " + type);
                                    break;
                              }
                           }
                        }
                     }
                     
                     if (ps.execute() && resultSet)
                     {
                        ResultSet rs = ps.getResultSet();
                        while (rs.next())
                        {
                           // Just advance
                        }
                        rs.close();
                     }

                     ps.close();
                  }
               }
            }
            
            afterRun = System.currentTimeMillis();
            success = true;
         }
         catch (Exception e)
         {
            System.out.println(de);
            e.printStackTrace();
         }
         finally
         {
            if (c != null)
            {
               try
               {
                  c.close();
               }
               catch (Exception e)
               {
                  // Nothing to do
               }
            }
            afterConnection = System.currentTimeMillis();
            clientDone.countDown();
         }
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

   /**
    * Data entry
    */
   static class DataEntry
   {
      private boolean prepared;
      private String statement;
      private List<Integer> types;
      private List<String> parameters;
      
      DataEntry()
      {
         prepared = false;
         statement = null;
         types = null;
         parameters = null;
      }

      DataEntry(String p, String s, String t, String pa)
      {
         this();
         prepared = "P".equals(p);
         statement = s;
         if (t != null && !"".equals(t))
         {
            types = new ArrayList<>();
            StringTokenizer st = new StringTokenizer(t, "|");
            while (st.hasMoreTokens())
            {
               types.add(Integer.valueOf(st.nextToken()));
            }
         }
         if (pa != null && !"".equals(pa))
         {
            parameters = new ArrayList<>();
            StringTokenizer st = new StringTokenizer(pa, "|");
            while (st.hasMoreTokens())
            {
               parameters.add(st.nextToken());
            }
         }
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
       * Get types
       * @return The values
       */
      List<Integer> getTypes()
      {
         return types;
      }
      
      /**
       * Set types
       * @param v The value
       */
      void setTypes(List<Integer> v)
      {
         types = v;
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
       * Get data
       * @return The data
       */
      List<String> getData()
      {
         List<String> result = new ArrayList<>();

         result.add(prepared ? "P" : "S");
         result.add(statement);

         if (types != null)
         {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < types.size(); i++)
            {
               sb = sb.append(types.get(i));
               if (i < types.size() - 1)
                  sb = sb.append("|");
            }
            result.add(sb.toString());
         }
         else
         {
            result.add("");
         }

         if (parameters != null)
         {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parameters.size(); i++)
            {
               sb = sb.append(parameters.get(i));
               if (i < parameters.size() - 1)
                  sb = sb.append("|");
            }
            result.add(sb.toString());
         }
         else
         {
            result.add("");
         }
         
         return result;
      }

      /**
       * {@inheritDoc}
       */
      public String toString()
      {
         return getData().toString();
      }
   }

   /**
    * Basic Xid implementation
    */
   static class XidImpl implements Xid
   {
      private int id;

      /**
       * Constructor
       * @param id The identifier
       */
      public XidImpl(int id)
      {
         this.id = id;
      }

      /**
       * {@inheritDoc}
       */
      public int getFormatId()
      {
         return id;
      }

      /**
       * {@inheritDoc}
       */
      public byte[] getGlobalTransactionId()
      {
         return new byte[] {(byte)(id >>> 24), (byte)(id >>> 16), (byte)(id >>> 8), (byte)id};
      }

      /**
       * {@inheritDoc}
       */
      public byte[] getBranchQualifier()
      {
         return new byte[] {(byte)(id >>> 24), (byte)(id >>> 16), (byte)(id >>> 8), (byte)id};
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public int hashCode()
      {
         return id;
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public boolean equals(Object other)
      {
         if (other == this)
            return true;

         if (other == null || !(other instanceof XidImpl))
            return false;

         XidImpl x = (XidImpl)other;

         return id == x.id;
      }
   }
}
