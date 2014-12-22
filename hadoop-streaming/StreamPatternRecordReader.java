/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.streaming;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Treats keys as offset in file and value as line. 
 */
public class StreamPatternRecordReader implements RecordReader<Text, Text> {
    private CompressionCodecFactory compressionCodecs = null;
    private long start; 
    private long pos;
    private long end;
    private BufferedInputStream in;
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream(2048);
    private static byte[] pat = null;
    /**
     * Provide a bridge to get the bytes from the ByteArrayOutputStream
     * without creating a new byte array.
     */
    private static class TextStuffer extends OutputStream {
        public Text target;
        public void write(int b) {
            throw new UnsupportedOperationException("write(byte) not supported");
        }
        public void write(byte[] data, int offset, int len) throws IOException {
            target.set(data, offset, len);
        }      
    }
    private TextStuffer bridge = new TextStuffer();
  
    public StreamPatternRecordReader(FSDataInputStream in, FileSplit split, Reporter reporter,
            JobConf job, FileSystem fs) throws IOException {
        this(job, split);
    }
    /**
     * Initializes the pattern record reader placing the start position in the first
     * location of the matched pattern.
     */
    public StreamPatternRecordReader(Configuration job, FileSplit split)
            throws IOException {
        long start = split.getStart();
        long end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        InputStream in = fileIn;
        boolean skipFirstLine = false;
        if (codec != null) {
            in = codec.createInputStream(fileIn);
            end = Long.MAX_VALUE;
        } else if (start != 0) {
            skipFirstLine = true;  // wait till BufferedInputStream to skip
            fileIn.seek(start);
        }

        String val = job.get("stream.recordreader.begin");
        if (val == null) {
            throw new IOException("JobConf: missing required property: stream.recordreader.begin");
        }
        this.pat = val.getBytes();

        this.in = new BufferedInputStream(in);
        if (skipFirstLine) {  // skip first "block", which is really the end of block from the previous block, and re-establish "start".
            start += StreamPatternRecordReader.readBlock(this.in, null);
        }
        this.start = start;
        this.pos = start;
        this.end = end;
    }
  
    public StreamPatternRecordReader(InputStream in, long offset, long endOffset) 
            throws IOException{
        this.in = new BufferedInputStream(in);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;    
    }
  
    public Text createKey() {
        return new Text();
    }
  
    public Text createValue() {
        return new Text();
    }
  
    /** Read a block starting with pattern given. */
    public synchronized boolean next(Text key, Text value)
            throws IOException {
        if (pos >= end) {
            return false;
        }

        buffer.reset();
        long bytesRead = readBlock();
        if (0 == bytesRead) {
            return false;
        }
        pos += bytesRead;
        // Store the information block read as key (and not a value)
        bridge.target = key;
        buffer.writeTo(bridge);
        // Store empty string as value
        value.set("");
        return true;
    }
  
    /**
     * Reads the next block, looking for the pattern provided
     * using the internal buffer and previously set input stream.
     */
    protected long readBlock() throws IOException {
        return StreamPatternRecordReader.readBlock(in, buffer);
    }

    /**
     * Reads the next block, looking for the pattern provided.
     * @param in input stream from where data will be read
     * @param out output stream into which block will be written; null value indicates
     *            that the next block will just be read (useful when it is desirable
     *            to skip a block
     */
    public static long readBlock(InputStream in, 
            OutputStream out) throws IOException {
        long bytes = 0;
        int m = 0;
        boolean isFirst = true;
        if (null == out) {
            isFirst = false;
        }
        while (true) {
            if (0 == m) {
                in.mark(pat.length);
            }
            int b = in.read();
            if (b == -1) {
                break;
            }
            bytes++;
            
            byte c = (byte)b;
            if (c == pat[m]) {
                m++;
                if (m == pat.length) {
                    // Found the pattern for the first time.
                    if (isFirst) {
                        if (out != null) {
                            out.write(pat, 0, m);
                        }
                        isFirst = false;
                        m = 0;
                    } else {
                        bytes -= m;
                        in.reset();
                        break;
                    }
                }
            } else {
                if (out != null) {
                    out.write(pat, 0, m);
                    out.write(c);
                }
                m = 0;
                if (c == pat[m]) {
                    m++;
                }
            }
        }
        return bytes;
    }
  
    /**
     * Get the progress within the split
     */
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }
    
    public  synchronized long getPos() throws IOException {
        return pos;
    }
  
    public synchronized void close() throws IOException { 
        in.close(); 
    }
}
