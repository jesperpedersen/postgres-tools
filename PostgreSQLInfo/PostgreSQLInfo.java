/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 Jesper Pedersen <jesper.pedersen@comcast.net>
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
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Generate a HTML report of the PostgreSQL configuration
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class PostgreSQLInfo
{
   /** Default version */
   private static String DEFAULT_VERSION = "11";

   /** Defaults */
   private static Map<String, Map<String, String>> defaults = new TreeMap<>();

   /**
    * Read data from a file
    * @param p The path of the file
    * @return The data
    */
   private static List<String> readFile(Path p) throws Exception
   {
      return Files.readAllLines(p);
   }

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
    * Write index.html
    */
   private static void writeReport(String dataPath, String walPath,
                                   List<String> pgHbaConf, SortedMap<String, String> postgresqlConf,
                                   String version) throws Exception
   {
      List<String> l = new ArrayList<>();
      Map<String, String> m = defaults.get(version);

      l.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
      l.add("                      \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
      l.add("");
      l.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">");
      l.add("<head>");
      l.add("  <title>PostgreSQL Information</title>");
      l.add("  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>");
      l.add("</head>");
      l.add("<body>");
      l.add("<h1>PostgreSQL Information</h1>");
      l.add("");

      l.add("DATA: " + dataPath);
      l.add("<br/>");
      l.add("WAL: " + walPath);
      l.add("<br/>");

      l.add("<h2>pg_hba.conf</h2>");
      l.add("<pre>");
      for (String s : pgHbaConf)
      {
         s = s.replaceAll("\t", " ");
         s = s.replaceAll(" [ ]*", " ");
         l.add(s);
      }
      l.add("</pre>");
      
      l.add("<h2>postgresql.conf</h2>");
      l.add("<table>");
      l.add("  <thead>");
      l.add("    <tr align=\"left\">");
      l.add("      <th><b>Key</b></th>");
      l.add("      <th><b>Value</b></th>");
      l.add("      <th><b>Default</b></th>");
      l.add("    </tr>");
      l.add("  </thead>");

      l.add("  <tbody>");
      for (Map.Entry<String, String> entry : postgresqlConf.entrySet())
      {
         String defaultValue = m.get(entry.getKey());
         String value = entry.getValue();

         if (value.startsWith("\'"))
         {
            value = value.substring(1, value.length() - 1);
         }

         if (value.equals(defaultValue))
         {
            l.add("    <tr align=\"left\">");
            l.add("      <td>" + entry.getKey() + "</td>");
            l.add("      <td>" + entry.getValue() + "</td>");
            l.add("      <td></td>");
            l.add("    </tr>");
         }
         else
         {
            l.add("    <tr align=\"left\">");
            l.add("      <td><b>" + entry.getKey() + "</b></td>");
            l.add("      <td><b>" + entry.getValue() + "</b></td>");
            l.add("      <td>" + (defaultValue != null ? defaultValue : "") + "</td>");
            l.add("    </tr>");
         }
      }
      l.add("  </tbody>");

      l.add("</table>");

      l.add("");
      l.add("<p/>");
      l.add("<div align=\"right\">");
      l.add("Generated on: " + new Date().toString());
      l.add("</div>");
      
      l.add("</body>");
      l.add("</html>");

      writeFile(Paths.get("report", "index.html"), l);
   }

   /**
    * Parse access file
    * @param p The path
    * @return The data
    */
   private static List<String> parseAccess(Path p) throws Exception
   {
      List<String> l = new ArrayList<>();
      List<String> content = readFile(p);

      for (String s : content)
      {
         s = s.trim();
         if (!"".equals(s) && !s.startsWith("#"))
         {
            l.add(s);
         }
      }

      return l;
   }

   /**
    * Parse configuration file
    * @param p The path
    * @return The data
    */
   private static SortedMap<String, String> parseConfiguration(Path p) throws Exception
   {
      SortedMap<String, String> sm = new TreeMap<>();
      List<String> content = readFile(p);

      for (String s : content)
      {
         s = s.trim();
         if (!"".equals(s) && !s.startsWith("#"))
         {
            if (s.startsWith("include"))
            {
               if (s.startsWith("include_dir"))
               {
                  String directoryName = s.substring(s.indexOf("'") + 1, s.lastIndexOf("'"));
                  if (!directoryName.startsWith(File.pathSeparator))
                     directoryName = p.toAbsolutePath().toString() + File.pathSeparator + directoryName;

                  File directory = new File(directoryName);
                  for (File f : directory.listFiles())
                  {
                     sm.putAll(parseConfiguration(f.toPath()));
                  }
               }
               else if (s.startsWith("include_if_exists"))
               {
                  String fileName = s.substring(s.indexOf("'") + 1, s.lastIndexOf("'"));
                  Path include = null;
                  if (!fileName.startsWith(File.pathSeparator))
                  {
                     include = Paths.get(p.toAbsolutePath().toString(), fileName);
                  }
                  else
                  {
                     include = Paths.get(fileName);
                  }

                  if (include.toFile().exists())
                     sm.putAll(parseConfiguration(include));
               }
               else
               {
                  String fileName = s.substring(s.indexOf("'") + 1, s.lastIndexOf("'"));
                  Path include = null;
                  if (!fileName.startsWith(File.pathSeparator))
                  {
                     include = Paths.get(p.toAbsolutePath().toString(), fileName);
                  }
                  else
                  {
                     include = Paths.get(fileName);
                  }

                  sm.putAll(parseConfiguration(include));
               }
            }
            else
            {
               int index = s.indexOf("=");
               String key = s.substring(0, index);
               String value = s.substring(index + 1);

               if (value.indexOf("#") != -1)
                  value = value.substring(0, value.indexOf("#"));

               key = key.trim();
               value = value.trim();
                  
               sm.put(key, value);
            }
         }
      }

      return sm;
   }

   /**
    * Setup defaults
    */
   private static void setupDefaults()
   {
      setupPostgreSQL11();
      setupPostgreSQL10();
      setupPostgreSQL96();
      setupPostgreSQL95();
      setupPostgreSQL94();
      setupPostgreSQL93();
      setupPostgreSQL92();
   }

   /**
    * Setup: PostgreSQL 11
    */
   private static void setupPostgreSQL11()
   {
      Map<String, String> m = new TreeMap<>();

      m.put("allow_system_table_mods", "off");
      m.put("application_name", "");
      m.put("archive_command", "");
      m.put("archive_mode", "off");
      m.put("archive_timeout", "0");
      m.put("array_nulls", "on");
      m.put("authentication_timeout", "1min");
      m.put("autovacuum", "on");
      m.put("autovacuum_analyze_scale_factor", "0.1");
      m.put("autovacuum_analyze_threshold", "50");
      m.put("autovacuum_freeze_max_age", "200000000");
      m.put("autovacuum_max_workers", "3");
      m.put("autovacuum_multixact_freeze_max_age", "400000000");
      m.put("autovacuum_naptime", "1min");
      m.put("autovacuum_vacuum_cost_delay", "20ms");
      m.put("autovacuum_vacuum_cost_limit", "-1");
      m.put("autovacuum_vacuum_scale_factor", "0.2");
      m.put("autovacuum_vacuum_threshold", "50");
      m.put("autovacuum_work_mem", "-1");
      m.put("backend_flush_after", "0");
      m.put("backslash_quote", "safe_encoding");
      m.put("bgwriter_delay", "200ms");
      m.put("bgwriter_flush_after", "512kB");
      m.put("bgwriter_lru_maxpages", "100");
      m.put("bgwriter_lru_multiplier", "2");
      m.put("block_size", "8192");
      m.put("bonjour", "off");
      m.put("bonjour_name", "");
      m.put("bytea_output", "hex");
      m.put("check_function_bodies", "on");
      m.put("checkpoint_completion_target", "0.5");
      m.put("checkpoint_flush_after", "256kB");
      m.put("checkpoint_timeout", "5min");
      m.put("checkpoint_warning", "30s");
      m.put("client_encoding", "UTF8");
      m.put("client_min_messages", "notice");
      m.put("cluster_name", "");
      m.put("commit_delay", "0");
      m.put("commit_siblings", "5");
      m.put("constraint_exclusion", "partition");
      m.put("cpu_index_tuple_cost", "0.005");
      m.put("cpu_operator_cost", "0.0025");
      m.put("cpu_tuple_cost", "0.01");
      m.put("cursor_tuple_fraction", "0.1");
      m.put("data_checksums", "off");
      m.put("data_directory_mode", "0700");
      m.put("DateStyle", "ISO, MDY");
      m.put("db_user_namespace", "off");
      m.put("deadlock_timeout", "1s");
      m.put("debug_assertions", "off");
      m.put("debug_pretty_print", "on");
      m.put("debug_print_parse", "off");
      m.put("debug_print_plan", "off");
      m.put("debug_print_rewritten", "off");
      m.put("default_statistics_target", "100");
      m.put("default_tablespace", "");
      m.put("default_text_search_config", "pg_catalog.english");
      m.put("default_transaction_deferrable", "off");
      m.put("default_transaction_isolation", "read committed");
      m.put("default_transaction_read_only", "off");
      m.put("default_with_oids", "off");
      m.put("dynamic_shared_memory_type", "posix");
      m.put("effective_cache_size", "4GB");
      m.put("effective_io_concurrency", "1");
      m.put("enable_bitmapscan", "on");
      m.put("enable_gathermerge", "on");
      m.put("enable_hashagg", "on");
      m.put("enable_hashjoin", "on");
      m.put("enable_indexonlyscan", "on");
      m.put("enable_indexscan", "on");
      m.put("enable_material", "on");
      m.put("enable_mergejoin", "on");
      m.put("enable_nestloop", "on");
      m.put("enable_parallel_append", "on");
      m.put("enable_parallel_hash", "on");
      m.put("enable_partition_pruning", "on");
      m.put("enable_partitionwise_aggregate", "off");
      m.put("enable_partitionwise_join", "off");
      m.put("enable_seqscan", "on");
      m.put("enable_sort", "on");
      m.put("enable_tidscan", "on");
      m.put("escape_string_warning", "on");
      m.put("event_source", "PostgreSQL");
      m.put("exit_on_error", "off");
      m.put("extra_float_digits", "3");
      m.put("force_parallel_mode", "off");
      m.put("from_collapse_limit", "8");
      m.put("fsync", "on");
      m.put("full_page_writes", "on");
      m.put("geqo", "on");
      m.put("geqo_effort", "5");
      m.put("geqo_generations", "0");
      m.put("geqo_pool_size", "0");
      m.put("geqo_seed", "0");
      m.put("geqo_selection_bias", "2");
      m.put("geqo_threshold", "12");
      m.put("gin_fuzzy_search_limit", "0");
      m.put("gin_pending_list_limit", "4MB");
      m.put("hot_standby", "on");
      m.put("hot_standby_feedback", "off");
      m.put("huge_pages", "try");
      m.put("idle_in_transaction_session_timeout", "0");
      m.put("ignore_checksum_failure", "off");
      m.put("ignore_system_indexes", "off");
      m.put("integer_datetimes", "on");
      m.put("IntervalStyle", "postgres");
      m.put("jit", "off");
      m.put("jit_above_cost", "100000");
      m.put("jit_debugging_support", "off");
      m.put("jit_dump_bitcode", "off");
      m.put("jit_expressions", "on");
      m.put("jit_inline_above_cost", "500000");
      m.put("jit_optimize_above_cost", "500000");
      m.put("jit_profiling_support", "off");
      m.put("jit_tuple_deforming", "on");
      m.put("join_collapse_limit", "8");
      m.put("krb_caseins_users", "off");
      m.put("lc_collate", "en_US.UTF-8");
      m.put("lc_ctype", "en_US.UTF-8");
      m.put("lc_messages", "en_US.UTF-8");
      m.put("lc_monetary", "en_US.UTF-8");
      m.put("lc_numeric", "en_US.UTF-8");
      m.put("lc_time", "en_US.UTF-8");
      m.put("listen_addresses", "localhost");
      m.put("lo_compat_privileges", "off");
      m.put("local_preload_libraries", "");
      m.put("lock_timeout", "0");
      m.put("log_autovacuum_min_duration", "-1");
      m.put("log_checkpoints", "off");
      m.put("log_connections", "off");
      m.put("log_destination", "stderr");
      m.put("log_disconnections", "off");
      m.put("log_duration", "off");
      m.put("log_error_verbosity", "default");
      m.put("log_executor_stats", "off");
      m.put("log_file_mode", "0600");
      m.put("log_hostname", "off");
      m.put("log_line_prefix", "%m [%p] ");
      m.put("log_lock_waits", "off");
      m.put("log_min_duration_statement", "-1");
      m.put("log_min_error_statement", "error");
      m.put("log_min_messages", "warning");
      m.put("log_parser_stats", "off");
      m.put("log_planner_stats", "off");
      m.put("log_replication_commands", "off");
      m.put("log_rotation_age", "1d");
      m.put("log_rotation_size", "10MB");
      m.put("log_statement", "none");
      m.put("log_statement_stats", "off");
      m.put("log_temp_files", "-1");
      m.put("log_timezone", "US/Eastern");
      m.put("log_truncate_on_rotation", "off");
      m.put("logging_collector", "off");
      m.put("maintenance_work_mem", "64MB");
      m.put("max_connections", "100");
      m.put("max_files_per_process", "1000");
      m.put("max_function_args", "100");
      m.put("max_identifier_length", "63");
      m.put("max_index_keys", "32");
      m.put("max_locks_per_transaction", "64");
      m.put("max_logical_replication_workers", "4");
      m.put("max_parallel_maintenance_workers", "2");
      m.put("max_parallel_workers", "8");
      m.put("max_parallel_workers_per_gather", "2");
      m.put("max_pred_locks_per_page", "2");
      m.put("max_pred_locks_per_relation", "-2");
      m.put("max_pred_locks_per_transaction", "64");
      m.put("max_prepared_transactions", "0");
      m.put("max_replication_slots", "10");
      m.put("max_stack_depth", "2MB");
      m.put("max_standby_archive_delay", "30s");
      m.put("max_standby_streaming_delay", "30s");
      m.put("max_sync_workers_per_subscription", "2");
      m.put("max_wal_senders", "10");
      m.put("max_wal_size", "1GB");
      m.put("max_worker_processes", "8");
      m.put("min_parallel_index_scan_size", "512kB");
      m.put("min_parallel_table_scan_size", "8MB");
      m.put("min_wal_size", "80MB");
      m.put("old_snapshot_threshold", "-1");
      m.put("operator_precedence_warning", "off");
      m.put("parallel_leader_participation", "on");
      m.put("parallel_setup_cost", "1000");
      m.put("parallel_tuple_cost", "0.1");
      m.put("password_encryption", "md5");
      m.put("port", "5432");
      m.put("post_auth_delay", "0");
      m.put("pre_auth_delay", "0");
      m.put("quote_all_identifiers", "off");
      m.put("random_page_cost", "4");
      m.put("restart_after_crash", "on");
      m.put("row_security", "on");
      m.put("search_path", "\"$user\", public");
      m.put("segment_size", "1GB");
      m.put("seq_page_cost", "1");
      m.put("server_encoding", "UTF8");
      m.put("server_version", "11.0");
      m.put("server_version_num", "110000");
      m.put("session_replication_role", "origin");
      m.put("shared_buffers", "128MB");
      m.put("ssl", "off");
      m.put("ssl_ca_file", "");
      m.put("ssl_cert_file", "server.crt");
      m.put("ssl_crl_file", "");
      m.put("ssl_key_file", "server.key");
      m.put("ssl_passphrase_command", "");
      m.put("ssl_passphrase_command_supports_reload", "off");
      m.put("ssl_prefer_server_ciphers", "on");
      m.put("standard_conforming_strings", "on");
      m.put("statement_timeout", "0");
      m.put("superuser_reserved_connections", "3");
      m.put("synchronize_seqscans", "on");
      m.put("synchronous_commit", "on");
      m.put("synchronous_standby_names", "");
      m.put("syslog_facility", "local0");
      m.put("syslog_ident", "postgres");
      m.put("syslog_sequence_numbers", "on");
      m.put("syslog_split_messages", "on");
      m.put("tcp_keepalives_count", "9");
      m.put("tcp_keepalives_idle", "7200");
      m.put("tcp_keepalives_interval", "75");
      m.put("temp_buffers", "8MB");
      m.put("temp_file_limit", "-1");
      m.put("temp_tablespaces", "");
      m.put("TimeZone", "America/New_York");
      m.put("timezone_abbreviations", "Default");
      m.put("trace_notify", "off");
      m.put("trace_recovery_messages", "log");
      m.put("trace_sort", "off");
      m.put("track_activities", "on");
      m.put("track_activity_query_size", "1kB");
      m.put("track_commit_timestamp", "off");
      m.put("track_counts", "on");
      m.put("track_functions", "none");
      m.put("track_io_timing", "off");
      m.put("transaction_deferrable", "off");
      m.put("transaction_isolation", "read committed");
      m.put("transaction_read_only", "off");
      m.put("transform_null_equals", "off");
      m.put("unix_socket_group", "");
      m.put("unix_socket_permissions", "0777");
      m.put("update_process_title", "on");
      m.put("vacuum_cleanup_index_scale_factor", "0.1");
      m.put("vacuum_cost_delay", "0");
      m.put("vacuum_cost_limit", "200");
      m.put("vacuum_cost_page_dirty", "20");
      m.put("vacuum_cost_page_hit", "1");
      m.put("vacuum_cost_page_miss", "10");
      m.put("vacuum_defer_cleanup_age", "0");
      m.put("vacuum_freeze_min_age", "50000000");
      m.put("vacuum_freeze_table_age", "150000000");
      m.put("vacuum_multixact_freeze_min_age", "5000000");
      m.put("vacuum_multixact_freeze_table_age", "150000000");
      m.put("wal_block_size", "8192");
      m.put("wal_buffers", "4MB");
      m.put("wal_compression", "off");
      m.put("wal_consistency_checking", "");
      m.put("wal_keep_segments", "0");
      m.put("wal_level", "replica");
      m.put("wal_log_hints", "off");
      m.put("wal_receiver_status_interval", "10s");
      m.put("wal_receiver_timeout", "1min");
      m.put("wal_retrieve_retry_interval", "5s");
      m.put("wal_segment_size", "16MB");
      m.put("wal_sender_timeout", "1min");
      m.put("wal_sync_method", "fdatasync");
      m.put("wal_writer_delay", "200ms");
      m.put("wal_writer_flush_after", "1MB");
      m.put("work_mem", "4MB");
      m.put("xmlbinary", "base64");
      m.put("xmloption", "content");
      m.put("zero_damaged_pages", "off");

      defaults.put("11", m);
   }

   /**
    * Setup: PostgreSQL 10
    */
   private static void setupPostgreSQL10()
   {
      Map<String, String> m = new TreeMap<>();

      m.put("allow_system_table_mods", "off");
      m.put("application_name", "");
      m.put("archive_command", "");
      m.put("archive_mode", "off");
      m.put("archive_timeout", "0");
      m.put("array_nulls", "on");
      m.put("authentication_timeout", "1min");
      m.put("autovacuum", "on");
      m.put("autovacuum_analyze_scale_factor", "0.1");
      m.put("autovacuum_analyze_threshold", "50");
      m.put("autovacuum_freeze_max_age", "200000000");
      m.put("autovacuum_max_workers", "3");
      m.put("autovacuum_multixact_freeze_max_age", "400000000");
      m.put("autovacuum_naptime", "1min");
      m.put("autovacuum_vacuum_cost_delay", "20ms");
      m.put("autovacuum_vacuum_cost_limit", "-1");
      m.put("autovacuum_vacuum_scale_factor", "0.2");
      m.put("autovacuum_vacuum_threshold", "50");
      m.put("autovacuum_work_mem", "-1");
      m.put("backend_flush_after", "0");
      m.put("backslash_quote", "safe_encoding");
      m.put("bgwriter_delay", "200ms");
      m.put("bgwriter_flush_after", "512kB");
      m.put("bgwriter_lru_maxpages", "100");
      m.put("bgwriter_lru_multiplier", "2");
      m.put("block_size", "8192");
      m.put("bonjour", "off");
      m.put("bonjour_name", "");
      m.put("bytea_output", "hex");
      m.put("check_function_bodies", "on");
      m.put("checkpoint_completion_target", "0.5");
      m.put("checkpoint_flush_after", "256kB");
      m.put("checkpoint_timeout", "5min");
      m.put("checkpoint_warning", "30s");
      m.put("client_encoding", "UTF8");
      m.put("client_min_messages", "notice");
      m.put("cluster_name", "");
      m.put("commit_delay", "0");
      m.put("commit_siblings", "5");
      m.put("constraint_exclusion", "partition");
      m.put("cpu_index_tuple_cost", "0.005");
      m.put("cpu_operator_cost", "0.0025");
      m.put("cpu_tuple_cost", "0.01");
      m.put("cursor_tuple_fraction", "0.1");
      m.put("data_checksums", "off");
      m.put("DateStyle", "ISO, MDY");
      m.put("db_user_namespace", "off");
      m.put("deadlock_timeout", "1s");
      m.put("debug_assertions", "off");
      m.put("debug_pretty_print", "on");
      m.put("debug_print_parse", "off");
      m.put("debug_print_plan", "off");
      m.put("debug_print_rewritten", "off");
      m.put("default_statistics_target", "100");
      m.put("default_tablespace", "");
      m.put("default_text_search_config", "pg_catalog.english");
      m.put("default_transaction_deferrable", "off");
      m.put("default_transaction_isolation", "read committed");
      m.put("default_transaction_read_only", "off");
      m.put("default_with_oids", "off");
      m.put("dynamic_shared_memory_type", "posix");
      m.put("effective_cache_size", "4GB");
      m.put("effective_io_concurrency", "1");
      m.put("enable_bitmapscan", "on");
      m.put("enable_gathermerge", "on");
      m.put("enable_hashagg", "on");
      m.put("enable_hashjoin", "on");
      m.put("enable_indexonlyscan", "on");
      m.put("enable_indexscan", "on");
      m.put("enable_material", "on");
      m.put("enable_mergejoin", "on");
      m.put("enable_nestloop", "on");
      m.put("enable_seqscan", "on");
      m.put("enable_sort", "on");
      m.put("enable_tidscan", "on");
      m.put("escape_string_warning", "on");
      m.put("event_source", "PostgreSQL");
      m.put("exit_on_error", "off");
      m.put("extra_float_digits", "3");
      m.put("force_parallel_mode", "off");
      m.put("from_collapse_limit", "8");
      m.put("fsync", "on");
      m.put("full_page_writes", "on");
      m.put("geqo", "on");
      m.put("geqo_effort", "5");
      m.put("geqo_generations", "0");
      m.put("geqo_pool_size", "0");
      m.put("geqo_seed", "0");
      m.put("geqo_selection_bias", "2");
      m.put("geqo_threshold", "12");
      m.put("gin_fuzzy_search_limit", "0");
      m.put("gin_pending_list_limit", "4MB");
      m.put("hot_standby", "on");
      m.put("hot_standby_feedback", "off");
      m.put("huge_pages", "try");
      m.put("idle_in_transaction_session_timeout", "0");
      m.put("ignore_checksum_failure", "off");
      m.put("ignore_system_indexes", "off");
      m.put("integer_datetimes", "on");
      m.put("IntervalStyle", "postgres");
      m.put("join_collapse_limit", "8");
      m.put("krb_caseins_users", "off");
      m.put("lc_collate", "en_US.UTF-8");
      m.put("lc_ctype", "en_US.UTF-8");
      m.put("lc_messages", "en_US.UTF-8");
      m.put("lc_monetary", "en_US.UTF-8");
      m.put("lc_numeric", "en_US.UTF-8");
      m.put("lc_time", "en_US.UTF-8");
      m.put("listen_addresses", "localhost");
      m.put("lo_compat_privileges", "off");
      m.put("local_preload_libraries", "");
      m.put("lock_timeout", "0");
      m.put("log_autovacuum_min_duration", "-1");
      m.put("log_checkpoints", "off");
      m.put("log_connections", "off");
      m.put("log_destination", "stderr");
      m.put("log_disconnections", "off");
      m.put("log_duration", "off");
      m.put("log_error_verbosity", "default");
      m.put("log_executor_stats", "off");
      m.put("log_file_mode", "0600");
      m.put("log_hostname", "off");
      m.put("log_line_prefix", "%m [%p] ");
      m.put("log_lock_waits", "off");
      m.put("log_min_duration_statement", "-1");
      m.put("log_min_error_statement", "error");
      m.put("log_min_messages", "warning");
      m.put("log_parser_stats", "off");
      m.put("log_planner_stats", "off");
      m.put("log_replication_commands", "off");
      m.put("log_rotation_age", "1d");
      m.put("log_rotation_size", "10MB");
      m.put("log_statement", "none");
      m.put("log_statement_stats", "off");
      m.put("log_temp_files", "-1");
      m.put("log_timezone", "US/Eastern");
      m.put("log_truncate_on_rotation", "off");
      m.put("logging_collector", "off");
      m.put("maintenance_work_mem", "64MB");
      m.put("max_connections", "100");
      m.put("max_files_per_process", "1000");
      m.put("max_function_args", "100");
      m.put("max_identifier_length", "63");
      m.put("max_index_keys", "32");
      m.put("max_locks_per_transaction", "64");
      m.put("max_logical_replication_workers", "4");
      m.put("max_parallel_workers", "8");
      m.put("max_parallel_workers_per_gather", "2");
      m.put("max_pred_locks_per_page", "2");
      m.put("max_pred_locks_per_relation", "-2");
      m.put("max_pred_locks_per_transaction", "64");
      m.put("max_prepared_transactions", "0");
      m.put("max_replication_slots", "10");
      m.put("max_stack_depth", "2MB");
      m.put("max_standby_archive_delay", "30s");
      m.put("max_standby_streaming_delay", "30s");
      m.put("max_sync_workers_per_subscription", "2");
      m.put("max_wal_senders", "10");
      m.put("max_wal_size", "1GB");
      m.put("max_worker_processes", "8");
      m.put("min_parallel_index_scan_size", "512kB");
      m.put("min_parallel_table_scan_size", "8MB");
      m.put("min_wal_size", "80MB");
      m.put("old_snapshot_threshold", "-1");
      m.put("operator_precedence_warning", "off");
      m.put("parallel_setup_cost", "1000");
      m.put("parallel_tuple_cost", "0.1");
      m.put("password_encryption", "md5");
      m.put("port", "5432");
      m.put("post_auth_delay", "0");
      m.put("pre_auth_delay", "0");
      m.put("quote_all_identifiers", "off");
      m.put("random_page_cost", "4");
      m.put("replacement_sort_tuples", "150000");
      m.put("restart_after_crash", "on");
      m.put("row_security", "on");
      m.put("search_path", "\"$user\", public");
      m.put("segment_size", "1GB");
      m.put("seq_page_cost", "1");
      m.put("server_encoding", "UTF8");
      m.put("server_version", "10.5");
      m.put("server_version_num", "100005");
      m.put("session_replication_role", "origin");
      m.put("shared_buffers", "128MB");
      m.put("ssl", "off");
      m.put("ssl_ca_file", "");
      m.put("ssl_cert_file", "server.crt");
      m.put("ssl_crl_file", "");
      m.put("ssl_key_file", "server.key");
      m.put("ssl_prefer_server_ciphers", "on");
      m.put("standard_conforming_strings", "on");
      m.put("statement_timeout", "0");
      m.put("superuser_reserved_connections", "3");
      m.put("synchronize_seqscans", "on");
      m.put("synchronous_commit", "on");
      m.put("synchronous_standby_names", "");
      m.put("syslog_facility", "local0");
      m.put("syslog_ident", "postgres");
      m.put("syslog_sequence_numbers", "on");
      m.put("syslog_split_messages", "on");
      m.put("tcp_keepalives_count", "9");
      m.put("tcp_keepalives_idle", "7200");
      m.put("tcp_keepalives_interval", "75");
      m.put("temp_buffers", "8MB");
      m.put("temp_file_limit", "-1");
      m.put("temp_tablespaces", "");
      m.put("TimeZone", "America/New_York");
      m.put("timezone_abbreviations", "Default");
      m.put("trace_notify", "off");
      m.put("trace_recovery_messages", "log");
      m.put("trace_sort", "off");
      m.put("track_activities", "on");
      m.put("track_activity_query_size", "1024");
      m.put("track_commit_timestamp", "off");
      m.put("track_counts", "on");
      m.put("track_functions", "none");
      m.put("track_io_timing", "off");
      m.put("transaction_deferrable", "off");
      m.put("transaction_isolation", "read committed");
      m.put("transaction_read_only", "off");
      m.put("transform_null_equals", "off");
      m.put("unix_socket_group", "");
      m.put("unix_socket_permissions", "0777");
      m.put("update_process_title", "on");
      m.put("vacuum_cost_delay", "0");
      m.put("vacuum_cost_limit", "200");
      m.put("vacuum_cost_page_dirty", "20");
      m.put("vacuum_cost_page_hit", "1");
      m.put("vacuum_cost_page_miss", "10");
      m.put("vacuum_defer_cleanup_age", "0");
      m.put("vacuum_freeze_min_age", "50000000");
      m.put("vacuum_freeze_table_age", "150000000");
      m.put("vacuum_multixact_freeze_min_age", "5000000");
      m.put("vacuum_multixact_freeze_table_age", "150000000");
      m.put("wal_block_size", "8192");
      m.put("wal_buffers", "4MB");
      m.put("wal_compression", "off");
      m.put("wal_consistency_checking", "");
      m.put("wal_keep_segments", "0");
      m.put("wal_level", "replica");
      m.put("wal_log_hints", "off");
      m.put("wal_receiver_status_interval", "10s");
      m.put("wal_receiver_timeout", "1min");
      m.put("wal_retrieve_retry_interval", "5s");
      m.put("wal_segment_size", "16MB");
      m.put("wal_sender_timeout", "1min");
      m.put("wal_sync_method", "fdatasync");
      m.put("wal_writer_delay", "200ms");
      m.put("wal_writer_flush_after", "1MB");
      m.put("work_mem", "4MB");
      m.put("xmlbinary", "base64");
      m.put("xmloption", "content");
      m.put("zero_damaged_pages", "off");

      defaults.put("10", m);
   }

   /**
    * Setup: PostgreSQL 9.6
    */
   private static void setupPostgreSQL96()
   {
      Map<String, String> m = new TreeMap<>();

      m.put("allow_system_table_mods", "off");
      m.put("application_name", "");
      m.put("archive_command", "");
      m.put("archive_mode", "off");
      m.put("archive_timeout", "0");
      m.put("array_nulls", "on");
      m.put("authentication_timeout", "1min");
      m.put("autovacuum", "on");
      m.put("autovacuum_analyze_scale_factor", "0.1");
      m.put("autovacuum_analyze_threshold", "50");
      m.put("autovacuum_freeze_max_age", "200000000");
      m.put("autovacuum_max_workers", "3");
      m.put("autovacuum_multixact_freeze_max_age", "400000000");
      m.put("autovacuum_naptime", "1min");
      m.put("autovacuum_vacuum_cost_delay", "20ms");
      m.put("autovacuum_vacuum_cost_limit", "-1");
      m.put("autovacuum_vacuum_scale_factor", "0.2");
      m.put("autovacuum_vacuum_threshold", "50");
      m.put("autovacuum_work_mem", "-1");
      m.put("backend_flush_after", "0");
      m.put("backslash_quote", "safe_encoding");
      m.put("bgwriter_delay", "200ms");
      m.put("bgwriter_flush_after", "512kB");
      m.put("bgwriter_lru_maxpages", "100");
      m.put("bgwriter_lru_multiplier", "2");
      m.put("block_size", "8192");
      m.put("bonjour", "off");
      m.put("bonjour_name", "");
      m.put("bytea_output", "hex");
      m.put("check_function_bodies", "on");
      m.put("checkpoint_completion_target", "0.5");
      m.put("checkpoint_flush_after", "256kB");
      m.put("checkpoint_timeout", "5min");
      m.put("checkpoint_warning", "30s");
      m.put("client_encoding", "UTF8");
      m.put("client_min_messages", "notice");
      m.put("cluster_name", "");
      m.put("commit_delay", "0");
      m.put("commit_siblings", "5");
      m.put("constraint_exclusion", "partition");
      m.put("cpu_index_tuple_cost", "0.005");
      m.put("cpu_operator_cost", "0.0025");
      m.put("cpu_tuple_cost", "0.01");
      m.put("cursor_tuple_fraction", "0.1");
      m.put("data_checksums", "off");
      m.put("DateStyle", "ISO, MDY");
      m.put("db_user_namespace", "off");
      m.put("deadlock_timeout", "1s");
      m.put("debug_assertions", "off");
      m.put("debug_pretty_print", "on");
      m.put("debug_print_parse", "off");
      m.put("debug_print_plan", "off");
      m.put("debug_print_rewritten", "off");
      m.put("default_statistics_target", "100");
      m.put("default_tablespace", "");
      m.put("default_text_search_config", "pg_catalog.english");
      m.put("default_transaction_deferrable", "off");
      m.put("default_transaction_isolation", "read committed");
      m.put("default_transaction_read_only", "off");
      m.put("default_with_oids", "off");
      m.put("dynamic_shared_memory_type", "posix");
      m.put("effective_cache_size", "4GB");
      m.put("effective_io_concurrency", "1");
      m.put("enable_bitmapscan", "on");
      m.put("enable_hashagg", "on");
      m.put("enable_hashjoin", "on");
      m.put("enable_indexonlyscan", "on");
      m.put("enable_indexscan", "on");
      m.put("enable_material", "on");
      m.put("enable_mergejoin", "on");
      m.put("enable_nestloop", "on");
      m.put("enable_seqscan", "on");
      m.put("enable_sort", "on");
      m.put("enable_tidscan", "on");
      m.put("escape_string_warning", "on");
      m.put("event_source", "PostgreSQL");
      m.put("exit_on_error", "off");
      m.put("extra_float_digits", "3");
      m.put("force_parallel_mode", "off");
      m.put("from_collapse_limit", "8");
      m.put("fsync", "on");
      m.put("full_page_writes", "on");
      m.put("geqo", "on");
      m.put("geqo_effort", "5");
      m.put("geqo_generations", "0");
      m.put("geqo_pool_size", "0");
      m.put("geqo_seed", "0");
      m.put("geqo_selection_bias", "2");
      m.put("geqo_threshold", "12");
      m.put("gin_fuzzy_search_limit", "0");
      m.put("gin_pending_list_limit", "4MB");
      m.put("hot_standby", "off");
      m.put("hot_standby_feedback", "off");
      m.put("huge_pages", "try");
      m.put("idle_in_transaction_session_timeout", "0");
      m.put("ignore_checksum_failure", "off");
      m.put("ignore_system_indexes", "off");
      m.put("integer_datetimes", "on");
      m.put("IntervalStyle", "postgres");
      m.put("join_collapse_limit", "8");
      m.put("krb_caseins_users", "off");
      m.put("lc_collate", "en_US.UTF-8");
      m.put("lc_ctype", "en_US.UTF-8");
      m.put("lc_messages", "en_US.UTF-8");
      m.put("lc_monetary", "en_US.UTF-8");
      m.put("lc_numeric", "en_US.UTF-8");
      m.put("lc_time", "en_US.UTF-8");
      m.put("listen_addresses", "localhost");
      m.put("lo_compat_privileges", "off");
      m.put("local_preload_libraries", "");
      m.put("lock_timeout", "0");
      m.put("log_autovacuum_min_duration", "-1");
      m.put("log_checkpoints", "off");
      m.put("log_connections", "off");
      m.put("log_destination", "stderr");
      m.put("log_disconnections", "off");
      m.put("log_duration", "off");
      m.put("log_error_verbosity", "default");
      m.put("log_executor_stats", "off");
      m.put("log_file_mode", "0600");
      m.put("log_hostname", "off");
      m.put("log_line_prefix", "");
      m.put("log_lock_waits", "off");
      m.put("log_min_duration_statement", "-1");
      m.put("log_min_error_statement", "error");
      m.put("log_min_messages", "warning");
      m.put("log_parser_stats", "off");
      m.put("log_planner_stats", "off");
      m.put("log_replication_commands", "off");
      m.put("log_rotation_age", "1d");
      m.put("log_rotation_size", "10MB");
      m.put("log_statement", "none");
      m.put("log_statement_stats", "off");
      m.put("log_temp_files", "-1");
      m.put("log_timezone", "US/Eastern");
      m.put("log_truncate_on_rotation", "off");
      m.put("logging_collector", "off");
      m.put("maintenance_work_mem", "64MB");
      m.put("max_connections", "100");
      m.put("max_files_per_process", "1000");
      m.put("max_function_args", "100");
      m.put("max_identifier_length", "63");
      m.put("max_index_keys", "32");
      m.put("max_locks_per_transaction", "64");
      m.put("max_parallel_workers_per_gather", "0");
      m.put("max_pred_locks_per_transaction", "64");
      m.put("max_prepared_transactions", "0");
      m.put("max_replication_slots", "0");
      m.put("max_stack_depth", "2MB");
      m.put("max_standby_archive_delay", "30s");
      m.put("max_standby_streaming_delay", "30s");
      m.put("max_wal_senders", "0");
      m.put("max_wal_size", "1GB");
      m.put("max_worker_processes", "8");
      m.put("min_parallel_relation_size", "8MB");
      m.put("min_wal_size", "80MB");
      m.put("old_snapshot_threshold", "-1");
      m.put("operator_precedence_warning", "off");
      m.put("parallel_setup_cost", "1000");
      m.put("parallel_tuple_cost", "0.1");
      m.put("password_encryption", "on");
      m.put("port", "5432");
      m.put("post_auth_delay", "0");
      m.put("pre_auth_delay", "0");
      m.put("quote_all_identifiers", "off");
      m.put("random_page_cost", "4");
      m.put("replacement_sort_tuples", "150000");
      m.put("restart_after_crash", "on");
      m.put("row_security", "on");
      m.put("search_path", "\"$user\", public");
      m.put("segment_size", "1GB");
      m.put("seq_page_cost", "1");
      m.put("server_encoding", "UTF8");
      m.put("server_version", "9.6.10");
      m.put("server_version_num", "90610");
      m.put("session_replication_role", "origin");
      m.put("shared_buffers", "128MB");
      m.put("sql_inheritance", "on");
      m.put("ssl", "off");
      m.put("ssl_ca_file", "");
      m.put("ssl_cert_file", "server.crt");
      m.put("ssl_crl_file", "");
      m.put("ssl_key_file", "server.key");
      m.put("ssl_prefer_server_ciphers", "on");
      m.put("standard_conforming_strings", "on");
      m.put("statement_timeout", "0");
      m.put("superuser_reserved_connections", "3");
      m.put("synchronize_seqscans", "on");
      m.put("synchronous_commit", "on");
      m.put("synchronous_standby_names", "");
      m.put("syslog_facility", "local0");
      m.put("syslog_ident", "postgres");
      m.put("syslog_sequence_numbers", "on");
      m.put("syslog_split_messages", "on");
      m.put("tcp_keepalives_count", "9");
      m.put("tcp_keepalives_idle", "7200");
      m.put("tcp_keepalives_interval", "75");
      m.put("temp_buffers", "8MB");
      m.put("temp_file_limit", "-1");
      m.put("temp_tablespaces", "");
      m.put("TimeZone", "America/New_York");
      m.put("timezone_abbreviations", "Default");
      m.put("trace_notify", "off");
      m.put("trace_recovery_messages", "log");
      m.put("trace_sort", "off");
      m.put("track_activities", "on");
      m.put("track_activity_query_size", "1024");
      m.put("track_commit_timestamp", "off");
      m.put("track_counts", "on");
      m.put("track_functions", "none");
      m.put("track_io_timing", "off");
      m.put("transaction_deferrable", "off");
      m.put("transaction_isolation", "read committed");
      m.put("transaction_read_only", "off");
      m.put("transform_null_equals", "off");
      m.put("unix_socket_group", "");
      m.put("unix_socket_permissions", "0777");
      m.put("update_process_title", "on");
      m.put("vacuum_cost_delay", "0");
      m.put("vacuum_cost_limit", "200");
      m.put("vacuum_cost_page_dirty", "20");
      m.put("vacuum_cost_page_hit", "1");
      m.put("vacuum_cost_page_miss", "10");
      m.put("vacuum_defer_cleanup_age", "0");
      m.put("vacuum_freeze_min_age", "50000000");
      m.put("vacuum_freeze_table_age", "150000000");
      m.put("vacuum_multixact_freeze_min_age", "5000000");
      m.put("vacuum_multixact_freeze_table_age", "150000000");
      m.put("wal_block_size", "8192");
      m.put("wal_buffers", "4MB");
      m.put("wal_compression", "off");
      m.put("wal_keep_segments", "0");
      m.put("wal_level", "minimal");
      m.put("wal_log_hints", "off");
      m.put("wal_receiver_status_interval", "10s");
      m.put("wal_receiver_timeout", "1min");
      m.put("wal_retrieve_retry_interval", "5s");
      m.put("wal_segment_size", "16MB");
      m.put("wal_sender_timeout", "1min");
      m.put("wal_sync_method", "fdatasync");
      m.put("wal_writer_delay", "200ms");
      m.put("wal_writer_flush_after", "1MB");
      m.put("work_mem", "4MB");
      m.put("xmlbinary", "base64");
      m.put("xmloption", "content");
      m.put("zero_damaged_pages", "off");

      defaults.put("9.6", m);
   }

   /**
    * Setup: PostgreSQL 9.5
    */
   private static void setupPostgreSQL95()
   {
      Map<String, String> m = new TreeMap<>();

      m.put("allow_system_table_mods", "off");
      m.put("application_name", "");
      m.put("archive_command", "");
      m.put("archive_mode", "off");
      m.put("archive_timeout", "0");
      m.put("array_nulls", "on");
      m.put("authentication_timeout", "1min");
      m.put("autovacuum", "on");
      m.put("autovacuum_analyze_scale_factor", "0.1");
      m.put("autovacuum_analyze_threshold", "50");
      m.put("autovacuum_freeze_max_age", "200000000");
      m.put("autovacuum_max_workers", "3");
      m.put("autovacuum_multixact_freeze_max_age", "400000000");
      m.put("autovacuum_naptime", "1min");
      m.put("autovacuum_vacuum_cost_delay", "20ms");
      m.put("autovacuum_vacuum_cost_limit", "-1");
      m.put("autovacuum_vacuum_scale_factor", "0.2");
      m.put("autovacuum_vacuum_threshold", "50");
      m.put("autovacuum_work_mem", "-1");
      m.put("backslash_quote", "safe_encoding");
      m.put("bgwriter_delay", "200ms");
      m.put("bgwriter_lru_maxpages", "100");
      m.put("bgwriter_lru_multiplier", "2");
      m.put("block_size", "8192");
      m.put("bonjour", "off");
      m.put("bonjour_name", "");
      m.put("bytea_output", "hex");
      m.put("check_function_bodies", "on");
      m.put("checkpoint_completion_target", "0.5");
      m.put("checkpoint_timeout", "5min");
      m.put("checkpoint_warning", "30s");
      m.put("client_encoding", "UTF8");
      m.put("client_min_messages", "notice");
      m.put("cluster_name", "");
      m.put("commit_delay", "0");
      m.put("commit_siblings", "5");
      m.put("constraint_exclusion", "partition");
      m.put("cpu_index_tuple_cost", "0.005");
      m.put("cpu_operator_cost", "0.0025");
      m.put("cpu_tuple_cost", "0.01");
      m.put("cursor_tuple_fraction", "0.1");
      m.put("data_checksums", "off");
      m.put("DateStyle", "ISO, MDY");
      m.put("db_user_namespace", "off");
      m.put("deadlock_timeout", "1s");
      m.put("debug_assertions", "off");
      m.put("debug_pretty_print", "on");
      m.put("debug_print_parse", "off");
      m.put("debug_print_plan", "off");
      m.put("debug_print_rewritten", "off");
      m.put("default_statistics_target", "100");
      m.put("default_tablespace", "");
      m.put("default_text_search_config", "pg_catalog.english");
      m.put("default_transaction_deferrable", "off");
      m.put("default_transaction_isolation", "read committed");
      m.put("default_transaction_read_only", "off");
      m.put("default_with_oids", "off");
      m.put("dynamic_shared_memory_type", "posix");
      m.put("effective_cache_size", "4GB");
      m.put("effective_io_concurrency", "1");
      m.put("enable_bitmapscan", "on");
      m.put("enable_hashagg", "on");
      m.put("enable_hashjoin", "on");
      m.put("enable_indexonlyscan", "on");
      m.put("enable_indexscan", "on");
      m.put("enable_material", "on");
      m.put("enable_mergejoin", "on");
      m.put("enable_nestloop", "on");
      m.put("enable_seqscan", "on");
      m.put("enable_sort", "on");
      m.put("enable_tidscan", "on");
      m.put("escape_string_warning", "on");
      m.put("event_source", "PostgreSQL");
      m.put("exit_on_error", "off");
      m.put("extra_float_digits", "3");
      m.put("from_collapse_limit", "8");
      m.put("fsync", "on");
      m.put("full_page_writes", "on");
      m.put("geqo", "on");
      m.put("geqo_effort", "5");
      m.put("geqo_generations", "0");
      m.put("geqo_pool_size", "0");
      m.put("geqo_seed", "0");
      m.put("geqo_selection_bias", "2");
      m.put("geqo_threshold", "12");
      m.put("gin_fuzzy_search_limit", "0");
      m.put("gin_pending_list_limit", "4MB");
      m.put("hot_standby", "off");
      m.put("hot_standby_feedback", "off");
      m.put("huge_pages", "try");
      m.put("ignore_checksum_failure", "off");
      m.put("ignore_system_indexes", "off");
      m.put("integer_datetimes", "on");
      m.put("IntervalStyle", "postgres");
      m.put("join_collapse_limit", "8");
      m.put("krb_caseins_users", "off");
      m.put("lc_collate", "en_US.UTF-8");
      m.put("lc_ctype", "en_US.UTF-8");
      m.put("lc_messages", "en_US.UTF-8");
      m.put("lc_monetary", "en_US.UTF-8");
      m.put("lc_numeric", "en_US.UTF-8");
      m.put("lc_time", "en_US.UTF-8");
      m.put("listen_addresses", "localhost");
      m.put("lo_compat_privileges", "off");
      m.put("local_preload_libraries", "");
      m.put("lock_timeout", "0");
      m.put("log_autovacuum_min_duration", "-1");
      m.put("log_checkpoints", "off");
      m.put("log_connections", "off");
      m.put("log_destination", "stderr");
      m.put("log_disconnections", "off");
      m.put("log_duration", "off");
      m.put("log_error_verbosity", "default");
      m.put("log_executor_stats", "off");
      m.put("log_file_mode", "0600");
      m.put("log_hostname", "off");
      m.put("log_line_prefix", "");
      m.put("log_lock_waits", "off");
      m.put("log_min_duration_statement", "-1");
      m.put("log_min_error_statement", "error");
      m.put("log_min_messages", "warning");
      m.put("log_parser_stats", "off");
      m.put("log_planner_stats", "off");
      m.put("log_replication_commands", "off");
      m.put("log_rotation_age", "1d");
      m.put("log_rotation_size", "10MB");
      m.put("log_statement", "none");
      m.put("log_statement_stats", "off");
      m.put("log_temp_files", "-1");
      m.put("log_timezone", "US/Eastern");
      m.put("log_truncate_on_rotation", "off");
      m.put("logging_collector", "off");
      m.put("maintenance_work_mem", "64MB");
      m.put("max_connections", "100");
      m.put("max_files_per_process", "1000");
      m.put("max_function_args", "100");
      m.put("max_identifier_length", "63");
      m.put("max_index_keys", "32");
      m.put("max_locks_per_transaction", "64");
      m.put("max_pred_locks_per_transaction", "64");
      m.put("max_prepared_transactions", "0");
      m.put("max_replication_slots", "0");
      m.put("max_stack_depth", "2MB");
      m.put("max_standby_archive_delay", "30s");
      m.put("max_standby_streaming_delay", "30s");
      m.put("max_wal_senders", "0");
      m.put("max_wal_size", "1GB");
      m.put("max_worker_processes", "8");
      m.put("min_wal_size", "80MB");
      m.put("operator_precedence_warning", "off");
      m.put("password_encryption", "on");
      m.put("port", "5432");
      m.put("post_auth_delay", "0");
      m.put("pre_auth_delay", "0");
      m.put("quote_all_identifiers", "off");
      m.put("random_page_cost", "4");
      m.put("restart_after_crash", "on");
      m.put("row_security", "on");
      m.put("search_path", "\"$user\", public");
      m.put("segment_size", "1GB");
      m.put("seq_page_cost", "1");
      m.put("server_encoding", "UTF8");
      m.put("server_version", "9.5.14");
      m.put("server_version_num", "90514");
      m.put("session_replication_role", "origin");
      m.put("shared_buffers", "128MB");
      m.put("sql_inheritance", "on");
      m.put("ssl", "off");
      m.put("ssl_ca_file", "");
      m.put("ssl_cert_file", "server.crt");
      m.put("ssl_crl_file", "");
      m.put("ssl_key_file", "server.key");
      m.put("ssl_prefer_server_ciphers", "on");
      m.put("standard_conforming_strings", "on");
      m.put("statement_timeout", "0");
      m.put("superuser_reserved_connections", "3");
      m.put("synchronize_seqscans", "on");
      m.put("synchronous_commit", "on");
      m.put("synchronous_standby_names", "");
      m.put("syslog_facility", "local0");
      m.put("syslog_ident", "postgres");
      m.put("tcp_keepalives_count", "9");
      m.put("tcp_keepalives_idle", "7200");
      m.put("tcp_keepalives_interval", "75");
      m.put("temp_buffers", "8MB");
      m.put("temp_file_limit", "-1");
      m.put("temp_tablespaces", "");
      m.put("TimeZone", "America/New_York");
      m.put("timezone_abbreviations", "Default");
      m.put("trace_notify", "off");
      m.put("trace_recovery_messages", "log");
      m.put("trace_sort", "off");
      m.put("track_activities", "on");
      m.put("track_activity_query_size", "1024");
      m.put("track_commit_timestamp", "off");
      m.put("track_counts", "on");
      m.put("track_functions", "none");
      m.put("track_io_timing", "off");
      m.put("transaction_deferrable", "off");
      m.put("transaction_isolation", "read committed");
      m.put("transaction_read_only", "off");
      m.put("transform_null_equals", "off");
      m.put("unix_socket_group", "");
      m.put("unix_socket_permissions", "0777");
      m.put("update_process_title", "on");
      m.put("vacuum_cost_delay", "0");
      m.put("vacuum_cost_limit", "200");
      m.put("vacuum_cost_page_dirty", "20");
      m.put("vacuum_cost_page_hit", "1");
      m.put("vacuum_cost_page_miss", "10");
      m.put("vacuum_defer_cleanup_age", "0");
      m.put("vacuum_freeze_min_age", "50000000");
      m.put("vacuum_freeze_table_age", "150000000");
      m.put("vacuum_multixact_freeze_min_age", "5000000");
      m.put("vacuum_multixact_freeze_table_age", "150000000");
      m.put("wal_block_size", "8192");
      m.put("wal_buffers", "4MB");
      m.put("wal_compression", "off");
      m.put("wal_keep_segments", "0");
      m.put("wal_level", "minimal");
      m.put("wal_log_hints", "off");
      m.put("wal_receiver_status_interval", "10s");
      m.put("wal_receiver_timeout", "1min");
      m.put("wal_retrieve_retry_interval", "5s");
      m.put("wal_segment_size", "16MB");
      m.put("wal_sender_timeout", "1min");
      m.put("wal_sync_method", "fdatasync");
      m.put("wal_writer_delay", "200ms");
      m.put("work_mem", "4MB");
      m.put("xmlbinary", "base64");
      m.put("xmloption", "content");
      m.put("zero_damaged_pages", "off");

      defaults.put("9.5", m);
   }

   /**
    * Setup: PostgreSQL 9.4
    */
   private static void setupPostgreSQL94()
   {
      Map<String, String> m = new TreeMap<>();

      m.put("allow_system_table_mods", "off");
      m.put("application_name", "");
      m.put("archive_command", "");
      m.put("archive_mode", "off");
      m.put("archive_timeout", "0");
      m.put("array_nulls", "on");
      m.put("authentication_timeout", "1min");
      m.put("autovacuum", "on");
      m.put("autovacuum_analyze_scale_factor", "0.1");
      m.put("autovacuum_analyze_threshold", "50");
      m.put("autovacuum_freeze_max_age", "200000000");
      m.put("autovacuum_max_workers", "3");
      m.put("autovacuum_multixact_freeze_max_age", "400000000");
      m.put("autovacuum_naptime", "1min");
      m.put("autovacuum_vacuum_cost_delay", "20ms");
      m.put("autovacuum_vacuum_cost_limit", "-1");
      m.put("autovacuum_vacuum_scale_factor", "0.2");
      m.put("autovacuum_vacuum_threshold", "50");
      m.put("autovacuum_work_mem", "-1");
      m.put("backslash_quote", "safe_encoding");
      m.put("bgwriter_delay", "200ms");
      m.put("bgwriter_lru_maxpages", "100");
      m.put("bgwriter_lru_multiplier", "2");
      m.put("block_size", "8192");
      m.put("bonjour", "off");
      m.put("bonjour_name", "");
      m.put("bytea_output", "hex");
      m.put("check_function_bodies", "on");
      m.put("checkpoint_completion_target", "0.5");
      m.put("checkpoint_segments", "3");
      m.put("checkpoint_timeout", "5min");
      m.put("checkpoint_warning", "30s");
      m.put("client_encoding", "UTF8");
      m.put("client_min_messages", "notice");
      m.put("commit_delay", "0");
      m.put("commit_siblings", "5");
      m.put("constraint_exclusion", "partition");
      m.put("cpu_index_tuple_cost", "0.005");
      m.put("cpu_operator_cost", "0.0025");
      m.put("cpu_tuple_cost", "0.01");
      m.put("cursor_tuple_fraction", "0.1");
      m.put("data_checksums", "off");
      m.put("DateStyle", "ISO, MDY");
      m.put("db_user_namespace", "off");
      m.put("deadlock_timeout", "1s");
      m.put("debug_assertions", "off");
      m.put("debug_pretty_print", "on");
      m.put("debug_print_parse", "off");
      m.put("debug_print_plan", "off");
      m.put("debug_print_rewritten", "off");
      m.put("default_statistics_target", "100");
      m.put("default_tablespace", "");
      m.put("default_text_search_config", "pg_catalog.english");
      m.put("default_transaction_deferrable", "off");
      m.put("default_transaction_isolation", "read committed");
      m.put("default_transaction_read_only", "off");
      m.put("default_with_oids", "off");
      m.put("dynamic_shared_memory_type", "posix");
      m.put("effective_cache_size", "4GB");
      m.put("effective_io_concurrency", "1");
      m.put("enable_bitmapscan", "on");
      m.put("enable_hashagg", "on");
      m.put("enable_hashjoin", "on");
      m.put("enable_indexonlyscan", "on");
      m.put("enable_indexscan", "on");
      m.put("enable_material", "on");
      m.put("enable_mergejoin", "on");
      m.put("enable_nestloop", "on");
      m.put("enable_seqscan", "on");
      m.put("enable_sort", "on");
      m.put("enable_tidscan", "on");
      m.put("escape_string_warning", "on");
      m.put("event_source", "PostgreSQL");
      m.put("exit_on_error", "off");
      m.put("extra_float_digits", "3");
      m.put("from_collapse_limit", "8");
      m.put("fsync", "on");
      m.put("full_page_writes", "on");
      m.put("geqo", "on");
      m.put("geqo_effort", "5");
      m.put("geqo_generations", "0");
      m.put("geqo_pool_size", "0");
      m.put("geqo_seed", "0");
      m.put("geqo_selection_bias", "2");
      m.put("geqo_threshold", "12");
      m.put("gin_fuzzy_search_limit", "0");
      m.put("hot_standby", "off");
      m.put("hot_standby_feedback", "off");
      m.put("huge_pages", "try");
      m.put("ignore_checksum_failure", "off");
      m.put("ignore_system_indexes", "off");
      m.put("integer_datetimes", "on");
      m.put("IntervalStyle", "postgres");
      m.put("join_collapse_limit", "8");
      m.put("krb_caseins_users", "off");
      m.put("lc_collate", "en_US.UTF-8");
      m.put("lc_ctype", "en_US.UTF-8");
      m.put("lc_messages", "en_US.UTF-8");
      m.put("lc_monetary", "en_US.UTF-8");
      m.put("lc_numeric", "en_US.UTF-8");
      m.put("lc_time", "en_US.UTF-8");
      m.put("listen_addresses", "localhost");
      m.put("lo_compat_privileges", "off");
      m.put("local_preload_libraries", "");
      m.put("lock_timeout", "0");
      m.put("log_autovacuum_min_duration", "-1");
      m.put("log_checkpoints", "off");
      m.put("log_connections", "off");
      m.put("log_destination", "stderr");
      m.put("log_disconnections", "off");
      m.put("log_duration", "off");
      m.put("log_error_verbosity", "default");
      m.put("log_executor_stats", "off");
      m.put("log_file_mode", "0600");
      m.put("log_hostname", "off");
      m.put("log_line_prefix", "");
      m.put("log_lock_waits", "off");
      m.put("log_min_duration_statement", "-1");
      m.put("log_min_error_statement", "error");
      m.put("log_min_messages", "warning");
      m.put("log_parser_stats", "off");
      m.put("log_planner_stats", "off");
      m.put("log_rotation_age", "1d");
      m.put("log_rotation_size", "10MB");
      m.put("log_statement", "none");
      m.put("log_statement_stats", "off");
      m.put("log_temp_files", "-1");
      m.put("log_timezone", "US/Eastern");
      m.put("log_truncate_on_rotation", "off");
      m.put("logging_collector", "off");
      m.put("maintenance_work_mem", "64MB");
      m.put("max_connections", "100");
      m.put("max_files_per_process", "1000");
      m.put("max_function_args", "100");
      m.put("max_identifier_length", "63");
      m.put("max_index_keys", "32");
      m.put("max_locks_per_transaction", "64");
      m.put("max_pred_locks_per_transaction", "64");
      m.put("max_prepared_transactions", "0");
      m.put("max_replication_slots", "0");
      m.put("max_stack_depth", "2MB");
      m.put("max_standby_archive_delay", "30s");
      m.put("max_standby_streaming_delay", "30s");
      m.put("max_wal_senders", "0");
      m.put("max_worker_processes", "8");
      m.put("password_encryption", "on");
      m.put("port", "5432");
      m.put("post_auth_delay", "0");
      m.put("pre_auth_delay", "0");
      m.put("quote_all_identifiers", "off");
      m.put("random_page_cost", "4");
      m.put("restart_after_crash", "on");
      m.put("search_path", "\"$user\",public");
      m.put("segment_size", "1GB");
      m.put("seq_page_cost", "1");
      m.put("server_encoding", "UTF8");
      m.put("server_version", "9.4.19");
      m.put("server_version_num", "90419");
      m.put("session_replication_role", "origin");
      m.put("shared_buffers", "128MB");
      m.put("sql_inheritance", "on");
      m.put("ssl", "off");
      m.put("ssl_ca_file", "");
      m.put("ssl_cert_file", "server.crt");
      m.put("ssl_crl_file", "");
      m.put("ssl_key_file", "server.key");
      m.put("ssl_prefer_server_ciphers", "on");
      m.put("ssl_renegotiation_limit", "0");
      m.put("standard_conforming_strings", "on");
      m.put("statement_timeout", "0");
      m.put("superuser_reserved_connections", "3");
      m.put("synchronize_seqscans", "on");
      m.put("synchronous_commit", "on");
      m.put("synchronous_standby_names", "");
      m.put("syslog_facility", "local0");
      m.put("syslog_ident", "postgres");
      m.put("tcp_keepalives_count", "9");
      m.put("tcp_keepalives_idle", "7200");
      m.put("tcp_keepalives_interval", "75");
      m.put("temp_buffers", "8MB");
      m.put("temp_file_limit", "-1");
      m.put("temp_tablespaces", "");
      m.put("TimeZone", "America/New_York");
      m.put("timezone_abbreviations", "Default");
      m.put("trace_notify", "off");
      m.put("trace_recovery_messages", "log");
      m.put("trace_sort", "off");
      m.put("track_activities", "on");
      m.put("track_activity_query_size", "1024");
      m.put("track_counts", "on");
      m.put("track_functions", "none");
      m.put("track_io_timing", "off");
      m.put("transaction_deferrable", "off");
      m.put("transaction_isolation", "read committed");
      m.put("transaction_read_only", "off");
      m.put("transform_null_equals", "off");
      m.put("unix_socket_group", "");
      m.put("unix_socket_permissions", "0777");
      m.put("update_process_title", "on");
      m.put("vacuum_cost_delay", "0");
      m.put("vacuum_cost_limit", "200");
      m.put("vacuum_cost_page_dirty", "20");
      m.put("vacuum_cost_page_hit", "1");
      m.put("vacuum_cost_page_miss", "10");
      m.put("vacuum_defer_cleanup_age", "0");
      m.put("vacuum_freeze_min_age", "50000000");
      m.put("vacuum_freeze_table_age", "150000000");
      m.put("vacuum_multixact_freeze_min_age", "5000000");
      m.put("vacuum_multixact_freeze_table_age", "150000000");
      m.put("wal_block_size", "8192");
      m.put("wal_buffers", "4MB");
      m.put("wal_keep_segments", "0");
      m.put("wal_level", "minimal");
      m.put("wal_log_hints", "off");
      m.put("wal_receiver_status_interval", "10s");
      m.put("wal_receiver_timeout", "1min");
      m.put("wal_segment_size", "16MB");
      m.put("wal_sender_timeout", "1min");
      m.put("wal_sync_method", "fdatasync");
      m.put("wal_writer_delay", "200ms");
      m.put("work_mem", "4MB");
      m.put("xmlbinary", "base64");
      m.put("xmloption", "content");
      m.put("zero_damaged_pages", "off");

      defaults.put("9.4", m);
   }

   /**
    * Setup: PostgreSQL 9.3
    */
   private static void setupPostgreSQL93()
   {
      Map<String, String> m = new TreeMap<>();

      m.put("allow_system_table_mods", "off");
      m.put("application_name", "");
      m.put("archive_command", "");
      m.put("archive_mode", "off");
      m.put("archive_timeout", "0");
      m.put("array_nulls", "on");
      m.put("authentication_timeout", "1min");
      m.put("autovacuum", "on");
      m.put("autovacuum_analyze_scale_factor", "0.1");
      m.put("autovacuum_analyze_threshold", "50");
      m.put("autovacuum_freeze_max_age", "200000000");
      m.put("autovacuum_max_workers", "3");
      m.put("autovacuum_multixact_freeze_max_age", "400000000");
      m.put("autovacuum_naptime", "1min");
      m.put("autovacuum_vacuum_cost_delay", "20ms");
      m.put("autovacuum_vacuum_cost_limit", "-1");
      m.put("autovacuum_vacuum_scale_factor", "0.2");
      m.put("autovacuum_vacuum_threshold", "50");
      m.put("backslash_quote", "safe_encoding");
      m.put("bgwriter_delay", "200ms");
      m.put("bgwriter_lru_maxpages", "100");
      m.put("bgwriter_lru_multiplier", "2");
      m.put("block_size", "8192");
      m.put("bonjour", "off");
      m.put("bonjour_name", "");
      m.put("bytea_output", "hex");
      m.put("check_function_bodies", "on");
      m.put("checkpoint_completion_target", "0.5");
      m.put("checkpoint_segments", "3");
      m.put("checkpoint_timeout", "5min");
      m.put("checkpoint_warning", "30s");
      m.put("client_encoding", "UTF8");
      m.put("client_min_messages", "notice");
      m.put("commit_delay", "0");
      m.put("commit_siblings", "5");
      m.put("constraint_exclusion", "partition");
      m.put("cpu_index_tuple_cost", "0.005");
      m.put("cpu_operator_cost", "0.0025");
      m.put("cpu_tuple_cost", "0.01");
      m.put("cursor_tuple_fraction", "0.1");
      m.put("data_checksums", "off");
      m.put("DateStyle", "ISO, MDY");
      m.put("db_user_namespace", "off");
      m.put("deadlock_timeout", "1s");
      m.put("debug_assertions", "off");
      m.put("debug_pretty_print", "on");
      m.put("debug_print_parse", "off");
      m.put("debug_print_plan", "off");
      m.put("debug_print_rewritten", "off");
      m.put("default_statistics_target", "100");
      m.put("default_tablespace", "");
      m.put("default_text_search_config", "pg_catalog.english");
      m.put("default_transaction_deferrable", "off");
      m.put("default_transaction_isolation", "read committed");
      m.put("default_transaction_read_only", "off");
      m.put("default_with_oids", "off");
      m.put("effective_cache_size", "128MB");
      m.put("effective_io_concurrency", "1");
      m.put("enable_bitmapscan", "on");
      m.put("enable_hashagg", "on");
      m.put("enable_hashjoin", "on");
      m.put("enable_indexonlyscan", "on");
      m.put("enable_indexscan", "on");
      m.put("enable_material", "on");
      m.put("enable_mergejoin", "on");
      m.put("enable_nestloop", "on");
      m.put("enable_seqscan", "on");
      m.put("enable_sort", "on");
      m.put("enable_tidscan", "on");
      m.put("escape_string_warning", "on");
      m.put("event_source", "PostgreSQL");
      m.put("exit_on_error", "off");
      m.put("extra_float_digits", "3");
      m.put("from_collapse_limit", "8");
      m.put("fsync", "on");
      m.put("full_page_writes", "on");
      m.put("geqo", "on");
      m.put("geqo_effort", "5");
      m.put("geqo_generations", "0");
      m.put("geqo_pool_size", "0");
      m.put("geqo_seed", "0");
      m.put("geqo_selection_bias", "2");
      m.put("geqo_threshold", "12");
      m.put("gin_fuzzy_search_limit", "0");
      m.put("hot_standby", "off");
      m.put("hot_standby_feedback", "off");
      m.put("ignore_checksum_failure", "off");
      m.put("ignore_system_indexes", "off");
      m.put("integer_datetimes", "on");
      m.put("IntervalStyle", "postgres");
      m.put("join_collapse_limit", "8");
      m.put("krb_caseins_users", "off");
      m.put("krb_srvname", "postgres");
      m.put("lc_collate", "en_US.UTF-8");
      m.put("lc_ctype", "en_US.UTF-8");
      m.put("lc_messages", "en_US.UTF-8");
      m.put("lc_monetary", "en_US.UTF-8");
      m.put("lc_numeric", "en_US.UTF-8");
      m.put("lc_time", "en_US.UTF-8");
      m.put("listen_addresses", "localhost");
      m.put("lo_compat_privileges", "off");
      m.put("local_preload_libraries", "");
      m.put("lock_timeout", "0");
      m.put("log_autovacuum_min_duration", "-1");
      m.put("log_checkpoints", "off");
      m.put("log_connections", "off");
      m.put("log_destination", "stderr");
      m.put("log_disconnections", "off");
      m.put("log_duration", "off");
      m.put("log_error_verbosity", "default");
      m.put("log_executor_stats", "off");
      m.put("log_file_mode", "0600");
      m.put("log_hostname", "off");
      m.put("log_line_prefix", "");
      m.put("log_lock_waits", "off");
      m.put("log_min_duration_statement", "-1");
      m.put("log_min_error_statement", "error");
      m.put("log_min_messages", "warning");
      m.put("log_parser_stats", "off");
      m.put("log_planner_stats", "off");
      m.put("log_rotation_age", "1d");
      m.put("log_rotation_size", "10MB");
      m.put("log_statement", "none");
      m.put("log_statement_stats", "off");
      m.put("log_temp_files", "-1");
      m.put("log_timezone", "US/Eastern");
      m.put("log_truncate_on_rotation", "off");
      m.put("logging_collector", "off");
      m.put("maintenance_work_mem", "16MB");
      m.put("max_connections", "100");
      m.put("max_files_per_process", "1000");
      m.put("max_function_args", "100");
      m.put("max_identifier_length", "63");
      m.put("max_index_keys", "32");
      m.put("max_locks_per_transaction", "64");
      m.put("max_pred_locks_per_transaction", "64");
      m.put("max_prepared_transactions", "0");
      m.put("max_stack_depth", "2MB");
      m.put("max_standby_archive_delay", "30s");
      m.put("max_standby_streaming_delay", "30s");
      m.put("max_wal_senders", "0");
      m.put("password_encryption", "on");
      m.put("port", "5432");
      m.put("post_auth_delay", "0");
      m.put("pre_auth_delay", "0");
      m.put("quote_all_identifiers", "off");
      m.put("random_page_cost", "4");
      m.put("restart_after_crash", "on");
      m.put("search_path", "\"$user\",public");
      m.put("segment_size", "1GB");
      m.put("seq_page_cost", "1");
      m.put("server_encoding", "UTF8");
      m.put("server_version", "9.3.24");
      m.put("server_version_num", "90324");
      m.put("session_replication_role", "origin");
      m.put("shared_buffers", "128MB");
      m.put("sql_inheritance", "on");
      m.put("ssl", "off");
      m.put("ssl_ca_file", "");
      m.put("ssl_cert_file", "server.crt");
      m.put("ssl_crl_file", "");
      m.put("ssl_key_file", "server.key");
      m.put("ssl_renegotiation_limit", "0");
      m.put("standard_conforming_strings", "on");
      m.put("statement_timeout", "0");
      m.put("superuser_reserved_connections", "3");
      m.put("synchronize_seqscans", "on");
      m.put("synchronous_commit", "on");
      m.put("synchronous_standby_names", "");
      m.put("syslog_facility", "local0");
      m.put("syslog_ident", "postgres");
      m.put("tcp_keepalives_count", "9");
      m.put("tcp_keepalives_idle", "7200");
      m.put("tcp_keepalives_interval", "75");
      m.put("temp_buffers", "8MB");
      m.put("temp_file_limit", "-1");
      m.put("temp_tablespaces", "");
      m.put("TimeZone", "America/New_York");
      m.put("timezone_abbreviations", "Default");
      m.put("trace_notify", "off");
      m.put("trace_recovery_messages", "log");
      m.put("trace_sort", "off");
      m.put("track_activities", "on");
      m.put("track_activity_query_size", "1024");
      m.put("track_counts", "on");
      m.put("track_functions", "none");
      m.put("track_io_timing", "off");
      m.put("transaction_deferrable", "off");
      m.put("transaction_isolation", "read committed");
      m.put("transaction_read_only", "off");
      m.put("transform_null_equals", "off");
      m.put("unix_socket_group", "");
      m.put("unix_socket_permissions", "0777");
      m.put("update_process_title", "on");
      m.put("vacuum_cost_delay", "0");
      m.put("vacuum_cost_limit", "200");
      m.put("vacuum_cost_page_dirty", "20");
      m.put("vacuum_cost_page_hit", "1");
      m.put("vacuum_cost_page_miss", "10");
      m.put("vacuum_defer_cleanup_age", "0");
      m.put("vacuum_freeze_min_age", "50000000");
      m.put("vacuum_freeze_table_age", "150000000");
      m.put("vacuum_multixact_freeze_min_age", "5000000");
      m.put("vacuum_multixact_freeze_table_age", "150000000");
      m.put("wal_block_size", "8192");
      m.put("wal_buffers", "4MB");
      m.put("wal_keep_segments", "0");
      m.put("wal_level", "minimal");
      m.put("wal_receiver_status_interval", "10s");
      m.put("wal_receiver_timeout", "1min");
      m.put("wal_segment_size", "16MB");
      m.put("wal_sender_timeout", "1min");
      m.put("wal_sync_method", "fdatasync");
      m.put("wal_writer_delay", "200ms");
      m.put("work_mem", "1MB");
      m.put("xmlbinary", "base64");
      m.put("xmloption", "content");
      m.put("zero_damaged_pages", "off");

      defaults.put("9.3", m);
   }

   /**
    * Setup: PostgreSQL 9.2
    */
   private static void setupPostgreSQL92()
   {
      Map<String, String> m = new TreeMap<>();

      m.put("allow_system_table_mods", "off");
      m.put("application_name", "");
      m.put("archive_command", "");
      m.put("archive_mode", "off");
      m.put("archive_timeout", "0");
      m.put("array_nulls", "on");
      m.put("authentication_timeout", "1min");
      m.put("autovacuum", "on");
      m.put("autovacuum_analyze_scale_factor", "0.1");
      m.put("autovacuum_analyze_threshold", "50");
      m.put("autovacuum_freeze_max_age", "200000000");
      m.put("autovacuum_max_workers", "3");
      m.put("autovacuum_naptime", "1min");
      m.put("autovacuum_vacuum_cost_delay", "20ms");
      m.put("autovacuum_vacuum_cost_limit", "-1");
      m.put("autovacuum_vacuum_scale_factor", "0.2");
      m.put("autovacuum_vacuum_threshold", "50");
      m.put("backslash_quote", "safe_encoding");
      m.put("bgwriter_delay", "200ms");
      m.put("bgwriter_lru_maxpages", "100");
      m.put("bgwriter_lru_multiplier", "2");
      m.put("block_size", "8192");
      m.put("bonjour", "off");
      m.put("bonjour_name", "");
      m.put("bytea_output", "hex");
      m.put("check_function_bodies", "on");
      m.put("checkpoint_completion_target", "0.5");
      m.put("checkpoint_segments", "3");
      m.put("checkpoint_timeout", "5min");
      m.put("checkpoint_warning", "30s");
      m.put("client_encoding", "UTF8");
      m.put("client_min_messages", "notice");
      m.put("commit_delay", "0");
      m.put("commit_siblings", "5");
      m.put("constraint_exclusion", "partition");
      m.put("cpu_index_tuple_cost", "0.005");
      m.put("cpu_operator_cost", "0.0025");
      m.put("cpu_tuple_cost", "0.01");
      m.put("cursor_tuple_fraction", "0.1");
      m.put("DateStyle", "ISO, MDY");
      m.put("db_user_namespace", "off");
      m.put("deadlock_timeout", "1s");
      m.put("debug_assertions", "off");
      m.put("debug_pretty_print", "on");
      m.put("debug_print_parse", "off");
      m.put("debug_print_plan", "off");
      m.put("debug_print_rewritten", "off");
      m.put("default_statistics_target", "100");
      m.put("default_tablespace", "");
      m.put("default_text_search_config", "pg_catalog.english");
      m.put("default_transaction_deferrable", "off");
      m.put("default_transaction_isolation", "read committed");
      m.put("default_transaction_read_only", "off");
      m.put("default_with_oids", "off");
      m.put("effective_cache_size", "128MB");
      m.put("effective_io_concurrency", "1");
      m.put("enable_bitmapscan", "on");
      m.put("enable_hashagg", "on");
      m.put("enable_hashjoin", "on");
      m.put("enable_indexonlyscan", "on");
      m.put("enable_indexscan", "on");
      m.put("enable_material", "on");
      m.put("enable_mergejoin", "on");
      m.put("enable_nestloop", "on");
      m.put("enable_seqscan", "on");
      m.put("enable_sort", "on");
      m.put("enable_tidscan", "on");
      m.put("escape_string_warning", "on");
      m.put("event_source", "PostgreSQL");
      m.put("exit_on_error", "off");
      m.put("extra_float_digits", "3");
      m.put("from_collapse_limit", "8");
      m.put("fsync", "on");
      m.put("full_page_writes", "on");
      m.put("geqo", "on");
      m.put("geqo_effort", "5");
      m.put("geqo_generations", "0");
      m.put("geqo_pool_size", "0");
      m.put("geqo_seed", "0");
      m.put("geqo_selection_bias", "2");
      m.put("geqo_threshold", "12");
      m.put("gin_fuzzy_search_limit", "0");
      m.put("hot_standby", "off");
      m.put("hot_standby_feedback", "off");
      m.put("ignore_system_indexes", "off");
      m.put("integer_datetimes", "on");
      m.put("IntervalStyle", "postgres");
      m.put("join_collapse_limit", "8");
      m.put("krb_caseins_users", "off");
      m.put("krb_srvname", "postgres");
      m.put("lc_collate", "en_US.UTF-8");
      m.put("lc_ctype", "en_US.UTF-8");
      m.put("lc_messages", "en_US.UTF-8");
      m.put("lc_monetary", "en_US.UTF-8");
      m.put("lc_numeric", "en_US.UTF-8");
      m.put("lc_time", "en_US.UTF-8");
      m.put("listen_addresses", "localhost");
      m.put("lo_compat_privileges", "off");
      m.put("local_preload_libraries", "");
      m.put("log_autovacuum_min_duration", "-1");
      m.put("log_checkpoints", "off");
      m.put("log_connections", "off");
      m.put("log_destination", "stderr");
      m.put("log_disconnections", "off");
      m.put("log_duration", "off");
      m.put("log_error_verbosity", "default");
      m.put("log_executor_stats", "off");
      m.put("log_file_mode", "0600");
      m.put("log_hostname", "off");
      m.put("log_line_prefix", "");
      m.put("log_lock_waits", "off");
      m.put("log_min_duration_statement", "-1");
      m.put("log_min_error_statement", "error");
      m.put("log_min_messages", "warning");
      m.put("log_parser_stats", "off");
      m.put("log_planner_stats", "off");
      m.put("log_rotation_age", "1d");
      m.put("log_rotation_size", "10MB");
      m.put("log_statement", "none");
      m.put("log_statement_stats", "off");
      m.put("log_temp_files", "-1");
      m.put("log_timezone", "US/Eastern");
      m.put("log_truncate_on_rotation", "off");
      m.put("logging_collector", "off");
      m.put("maintenance_work_mem", "16MB");
      m.put("max_connections", "100");
      m.put("max_files_per_process", "1000");
      m.put("max_function_args", "100");
      m.put("max_identifier_length", "63");
      m.put("max_index_keys", "32");
      m.put("max_locks_per_transaction", "64");
      m.put("max_pred_locks_per_transaction", "64");
      m.put("max_prepared_transactions", "0");
      m.put("max_stack_depth", "2MB");
      m.put("max_standby_archive_delay", "30s");
      m.put("max_standby_streaming_delay", "30s");
      m.put("max_wal_senders", "0");
      m.put("password_encryption", "on");
      m.put("port", "5432");
      m.put("post_auth_delay", "0");
      m.put("pre_auth_delay", "0");
      m.put("quote_all_identifiers", "off");
      m.put("random_page_cost", "4");
      m.put("replication_timeout", "1min");
      m.put("restart_after_crash", "on");
      m.put("search_path", "\"$user\",public");
      m.put("segment_size", "1GB");
      m.put("seq_page_cost", "1");
      m.put("server_encoding", "UTF8");
      m.put("server_version", "9.2.24");
      m.put("server_version_num", "90224");
      m.put("session_replication_role", "origin");
      m.put("shared_buffers", "32MB");
      m.put("sql_inheritance", "on");
      m.put("ssl", "off");
      m.put("ssl_ca_file", "");
      m.put("ssl_cert_file", "server.crt");
      m.put("ssl_crl_file", "");
      m.put("ssl_key_file", "server.key");
      m.put("ssl_renegotiation_limit", "0");
      m.put("standard_conforming_strings", "on");
      m.put("statement_timeout", "0");
      m.put("superuser_reserved_connections", "3");
      m.put("synchronize_seqscans", "on");
      m.put("synchronous_commit", "on");
      m.put("synchronous_standby_names", "");
      m.put("syslog_facility", "local0");
      m.put("syslog_ident", "postgres");
      m.put("tcp_keepalives_count", "9");
      m.put("tcp_keepalives_idle", "7200");
      m.put("tcp_keepalives_interval", "75");
      m.put("temp_buffers", "8MB");
      m.put("temp_file_limit", "-1");
      m.put("temp_tablespaces", "");
      m.put("TimeZone", "America/New_York");
      m.put("timezone_abbreviations", "Default");
      m.put("trace_notify", "off");
      m.put("trace_recovery_messages", "log");
      m.put("trace_sort", "off");
      m.put("track_activities", "on");
      m.put("track_activity_query_size", "1024");
      m.put("track_counts", "on");
      m.put("track_functions", "none");
      m.put("track_io_timing", "off");
      m.put("transaction_deferrable", "off");
      m.put("transaction_isolation", "read committed");
      m.put("transaction_read_only", "off");
      m.put("transform_null_equals", "off");
      m.put("unix_socket_group", "");
      m.put("unix_socket_permissions", "0777");
      m.put("update_process_title", "on");
      m.put("vacuum_cost_delay", "0");
      m.put("vacuum_cost_limit", "200");
      m.put("vacuum_cost_page_dirty", "20");
      m.put("vacuum_cost_page_hit", "1");
      m.put("vacuum_cost_page_miss", "10");
      m.put("vacuum_defer_cleanup_age", "0");
      m.put("vacuum_freeze_min_age", "50000000");
      m.put("vacuum_freeze_table_age", "150000000");
      m.put("wal_block_size", "8192");
      m.put("wal_buffers", "1MB");
      m.put("wal_keep_segments", "0");
      m.put("wal_level", "minimal");
      m.put("wal_receiver_status_interval", "10s");
      m.put("wal_segment_size", "16MB");
      m.put("wal_sync_method", "fdatasync");
      m.put("wal_writer_delay", "200ms");
      m.put("work_mem", "1MB");
      m.put("xmlbinary", "base64");
      m.put("xmloption", "content");
      m.put("zero_damaged_pages", "off");

      defaults.put("9.2", m);
   }

   /**
    * Setup
    */
   private static void setup() throws Exception
   {
      File report = new File("report");
      if (report.exists())
      {
         Files.walk(Paths.get("report"))
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      }
      report.mkdir();
   }

   /**
    * Main
    * @param args The arguments
    */
   public static void main(String[] args)
   {
      try
      {
         if (args.length < 1 || args.length > 2)
         {
            System.out.println("Usage: PostgreSQLInfo <path/to/data_directory> [version]");
            return;
         }

         setup();
         setupDefaults();

         String pathToDataDirectory = args[0];
         String version = args.length == 2 ? args[1] : DEFAULT_VERSION;

         if (defaults.get(version) == null)
         {
            System.out.println("Unknown version: " + version);
            System.out.println("Valid versions: 11, 10, 9.6, 9.5, 9.4, 9.3 and 9.2");
            return;
         }

         Path pathToWALDirectory = Paths.get(pathToDataDirectory, "pg_xlog");
         if (!Files.exists(pathToWALDirectory))
         {
            pathToWALDirectory = Paths.get(pathToDataDirectory, "pg_wal");
         }
         if (Files.isSymbolicLink(pathToWALDirectory))
         {
            pathToWALDirectory = Files.readSymbolicLink(pathToWALDirectory);
         }
         
         Path hc = Paths.get(pathToDataDirectory, "pg_hba.conf");
         Path pc = Paths.get(pathToDataDirectory, "postgresql.conf");

         List<String> pgHbaConf = parseAccess(hc);
         SortedMap<String, String> postgresqlConf = parseConfiguration(pc);

         writeReport(pathToDataDirectory, pathToWALDirectory.toAbsolutePath().toString(),
                     pgHbaConf, postgresqlConf, version);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
