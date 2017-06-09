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

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Creates flamegraph input files from
 * https://github.com/jesperpedersen/postgres/tree/lwstat
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class LWLockStats
{
   // SHACQ
   private static final String SHACQ = "SHACQ";

   // SHMAX
   private static final String SHMAX = "SHMAX";

   // EXACQ
   private static final String EXACQ = "EXACQ";

   // EXMAX
   private static final String EXMAX = "EXMAX";

   // BLK
   private static final String BLK = "BLK";

   // SPINDELAY
   private static final String SPINDELAY = "SPINDELAY";

   // DEQUE
   private static final String DEQUE = "DEQUE";

   // MAXW
   private static final String MAXW = "MAXW";

   /**
    * Append a string to a file
    * @param s The string
    * @param fw The file writer
    */
   private static void write(String s, FileWriter fw) throws Exception
   {
      for (int i = 0; i < s.length(); i++)
      {
         fw.write((int)s.charAt(i));
      }
      fw.write('\n');
   }

   public static void main(String[] args)
   {
      if (args.length == 0 || args.length > 2)
      {
         System.out.println("Usage: LWLockStats [-c] <log_file>");
         return;
      }

      String data = null;
      try
      {
         boolean combine = false;
         int arg = 0;
         Map<String, Map<String, Long>> m = new HashMap<String, Map<String, Long>>();

         if ("-c".equals(args[0]))
         {
            combine = true;
            arg++;
         }
         
         File input = new File(args[arg]);
         FileReader fr = new FileReader(input);
         LineNumberReader lnr = new LineNumberReader(fr, 8192);

         data = lnr.readLine();
         while (data != null)
         {
            if (data.startsWith("PID"))
            {
               StringTokenizer st = new StringTokenizer(data, " ");

               // PID
               st.nextToken();

               // PROCESS
               st.nextToken();

               // LWLOCK
               st.nextToken();

               String name = st.nextToken();
               String id = st.nextToken();
               if (!combine)
                  id = id.substring(0, id.length() - 1);

               // SHACQ
               st.nextToken();
               String shacqValue = st.nextToken();
            
               // SHMAX
               st.nextToken();
               String shmaxValue = st.nextToken();
            
               // EXACQ
               st.nextToken();
               String exacqValue = st.nextToken();
            
               // EXMAX
               st.nextToken();
               String exmaxValue = st.nextToken();
            
               // BLK
               st.nextToken();
               String blkValue = st.nextToken();
            
               // SPINDELAY
               st.nextToken();
               String spindelayValue = st.nextToken();
            
               // DEQUE
               st.nextToken(); st.nextToken();
               String dequeValue = st.nextToken();
            
               // MAXW
               st.nextToken();
               String maxwValue = st.nextToken();

               String key = name;
               if (!combine)
                  key += " " + id;

               Map<String, Long> entry = m.get(key);
               if (entry == null)
                  entry = new HashMap<String, Long>();

               // SHARED LOCK
               Long l = entry.get(SHACQ);
               if (l == null)
                  l = Long.valueOf(shacqValue);
               else
                  l = Long.valueOf(l.longValue() + Long.valueOf(shacqValue).longValue());
               entry.put(SHACQ, l);
            
               l = entry.get(SHMAX);
               if (l == null)
                  l = Long.valueOf(shmaxValue);
               else
               {
                  Long t = Long.valueOf(shmaxValue);
                  if (t.longValue() > l.longValue())
                     l = t;
               }
               entry.put(SHMAX, l);

               // EXCLUSIVE LOCK
               l = entry.get(EXACQ);
               if (l == null)
                  l = Long.valueOf(exacqValue);
               else
                  l = Long.valueOf(l.longValue() + Long.valueOf(exacqValue).longValue());
               entry.put(EXACQ, l);
            
               l = entry.get(EXMAX);
               if (l == null)
                  l = Long.valueOf(exmaxValue);
               else
               {
                  Long t = Long.valueOf(exmaxValue);
                  if (t.longValue() > l.longValue())
                     l = t;
               }
               entry.put(EXMAX, l);
            
               // MAX WAITERS
               l = entry.get(MAXW);
               if (l == null)
                  l = Long.valueOf(maxwValue);
               else
               {
                  Long t = Long.valueOf(maxwValue);
                  if (t.longValue() > l.longValue())
                     l = t;
               }
               entry.put(MAXW, l);

               // BLOCK
               l = entry.get(BLK);
               if (l == null)
                  l = Long.valueOf(blkValue);
               else
                  l = Long.valueOf(l.longValue() + Long.valueOf(blkValue).longValue());
               entry.put(BLK, l);

               // SPINDELAY
               l = entry.get(SPINDELAY);
               if (l == null)
                  l = Long.valueOf(spindelayValue);
               else
                  l = Long.valueOf(l.longValue() + Long.valueOf(spindelayValue).longValue());
               entry.put(SPINDELAY, l);

               m.put(key, entry);
            }
            
            data = lnr.readLine();
         }

         fr.close();

         File weight = new File("weight.txt");
         FileWriter weightFw = new FileWriter(weight);

         File exclusive = new File("exclusive.txt");
         FileWriter exclusiveFw = new FileWriter(exclusive);

         File shared = new File("shared.txt");
         FileWriter sharedFw = new FileWriter(shared);

         File block = new File("block.txt");
         FileWriter blockFw = new FileWriter(block);

         File spin = new File("spin.txt");
         FileWriter spinFw = new FileWriter(spin);

         for (Map.Entry<String, Map<String, Long>> e : m.entrySet())
         {
            String s = e.getKey();
            Map<String, Long> values = e.getValue();

            StringBuilder sb = new StringBuilder();

            sb.append("lwlock_stats;");

            Long max = values.get(MAXW);

            for (long counter = 1; counter <= max.longValue(); counter++)
            {
               sb.append(counter).append(";");
            }
            
            sb.append(s).append(";");
            sb.append(" ");

            Long shA = values.get(SHACQ);
            Long shM = values.get(SHMAX);
            Long exA = values.get(EXACQ);
            Long exM = values.get(EXMAX);
            Long b = values.get(BLK);
            Long sd = values.get(SPINDELAY);

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
      catch (Exception e)
      {
         System.out.println(data);
         e.printStackTrace();
      }
   }
}
