package au.gov.ga.conn4DUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;

public class JDBFException
    extends IOException  {

  public JDBFException(String s) {
    this(s, null);
  }

  public JDBFException(Throwable throwable) {
    this(throwable.getMessage(), throwable);
  }

  public JDBFException(String s, Throwable throwable) {
    super(s);
    detail = throwable;
  }

  public String getMessage() {
    if (detail == null) {
      return super.getMessage();
    }
    else {
      return super.getMessage();
    }
  }

  public void printStackTrace(PrintStream printstream) {
    if (detail == null) {
      super.printStackTrace(printstream);
      return;
    }
    PrintStream printstream1 = printstream;
    printstream1.println(this);
    detail.printStackTrace(printstream);
    return;
  }

  public void printStackTrace() {
    printStackTrace(System.err);
  }

  public void printStackTrace(PrintWriter printwriter) {
    if (detail == null) {
      super.printStackTrace(printwriter);
      return;
    }
    PrintWriter printwriter1 = printwriter;

    printwriter1.println(this);
    detail.printStackTrace(printwriter);
    return;
  }

  private Throwable detail;
}
