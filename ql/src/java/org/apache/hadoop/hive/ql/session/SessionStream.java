package org.apache.hadoop.hive.ql.session;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

/**
 * The Session uses this stream instead of PrintStream to prevent closing of System out and System err.
 * Ref: HIVE-21033
 */
public class SessionStream extends PrintStream {

  public SessionStream(OutputStream out, boolean autoFlush, String encoding) throws UnsupportedEncodingException {
    super(out, autoFlush, encoding);
  }

  @Override
  public void close() {
    if (out != System.out && out != System.err) {
      //Don't close if system out or system err
      super.close();
    }
  }

}
