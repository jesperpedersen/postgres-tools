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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.Process;
import java.lang.ProcessBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 * Resolve addresses in an input file for flamegraph using eu-addr2line
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class ResolveAddress
{
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
    * Resolve address
    * @param binary The binary
    * @param address The address
    * @return The resolved address
    */
   private static String resolveAddress(String binary, String address) throws Exception
   {
      int offset = address.indexOf("+0x");

      if (offset != -1)
      {
         String function = address.substring(0, offset);

         List<String> command = new ArrayList<>();
         command.add("eu-addr2line");
         command.add("-e");
         command.add(binary);
         command.add(address);
         
         ProcessBuilder pb = new ProcessBuilder(command);
         Process p = pb.start();

         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         InputStream is = p.getInputStream();
         int c = is.read();
         while (c != -1)
         {
            baos.write(c);
            c = is.read();
         }

         is.close();
         p.waitFor();

         String output = baos.toString();
         if (output.startsWith("/"))
         {
            return function + "|" + output.substring(output.lastIndexOf("/") + 1, output.length() - 1);
         }
      }

      return address;
   }
   
   /**
    * Generate
    * @param binary The binary
    * @param input The input
    * @param output The output
    */
   private static void generate(String binary, String input, String output) throws Exception
   {
      List<String> lines = readFile(Paths.get(input));
      List<String> data = new ArrayList<>();
      Map<String, String> resolved = new TreeMap<>();
      
      for (String d : lines)
      {
         StringBuilder sb = new StringBuilder();
         String scan = d.substring(0, d.indexOf(" "));
         String count = d.substring(d.indexOf(" ") + 1);

         StringTokenizer st = new StringTokenizer(scan, ";");
         while (st.hasMoreTokens())
         {
            String token = st.nextToken();
            if (!resolved.containsKey(token))
            {
               String resolve = resolveAddress(binary, token);

               resolved.put(token, resolve);

               sb.append(resolve);
            }
            else
            {
               sb.append(resolved.get(token));
            }
            sb.append(";");
         }

         sb.append(" ").append(count);
         data.add(sb.toString());
      }

      writeFile(Paths.get(output), data);
   }

   /**
    * Main
    * @param args The arguments
    */
   public static void main(String[] args)
   {
      try
      {
         if (args.length != 2)
         {
            System.out.println("Usage: ResolveAddress <path/to/binary_or_lib> <file>");
            return;
         }

         String binary = args[0];
         String inputFile = args[1];
         
         generate(binary, inputFile, inputFile + ".out");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
