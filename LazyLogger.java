import java.io.*;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: Nick Humrich
 * @date: 2/7/14
 */
public class LazyLogger {
  /*
  Feel free to remove all Logger stuff. Otherwise, you will have to import the logger
  */

  private static final Logger LOG = LogFactory.getLog(StatsLogger.class);
  private BlockingQueue<String> itemsToLog;
  private String fileName;

  public LazyLogger() {
    itemsToLog = new LinkedBlockingQueue<String>();
    Thread t = new LazyLoggerThread();
    t.start(); //spawns a new thread for lazy logging
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public void log(String message) {
    try {
      itemsToLog.add(message);
    }
    catch (IllegalStateException e) {
      //this exception is thrown when the queue is full
      // Shouldn't happen with a LinkedBlockingQueue, max size is 2^31
      if (LOG.isWarnEnabled()) {
        LOG.warn("Statistic Log queue is full.", e);
      }
    }
  }

  private class LazyLoggerThread extends Thread {

    public LazyLoggerThread() {
      super();
      super.setPriority(Thread.MIN_PRIORITY);
    }

    public void run() {
      String item;
      FileLock fileLock = null;
      PrintStream out = null;
      FileChannel fileChannel = null;
      try {
        while(true) {
          item = itemsToLog.take();
          try {

            File file = new File(fileName);
            if (!file.exists()) {
              //if file doesn't exist, create it
              file.createNewFile();
            }
            fileChannel = new RandomAccessFile(file, "rw").getChannel();

            fileLock = attemptToObtainFile(fileChannel);
            //obtained lock. Write to file
            out = new PrintStream(new FileOutputStream(file, true));
            out.println(item);
          }
          catch (IOException e) {
            if (LOG.isWarnEnabled()) {
              LOG.warn("Error with statistics log " + e.getMessage(), e);
            }
          }
          finally {
            if (out != null) {
              out.close();
            }try {
              if (fileLock != null) {
                fileLock.release();
              }
              if (fileChannel != null) {
                fileChannel.close();
              }
            } catch (IOException e) {
              if (LOG.isWarnEnabled()) {
                LOG.warn("Error with statistics log " + e.getMessage(), e);
              }
            }
          }
        }
      }
      catch (InterruptedException e) {
        //probably shutting down - close stuff in finally block
      }
      finally {
        if (out != null) {
          out.close();
        }try {
          if (fileLock != null) {
            fileLock.release();
          }
          if (fileChannel != null) {
            fileChannel.close();
          }
        } catch (IOException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Error with statistics log " + e.getMessage(), e);
          }
        }
      }
    }

    public FileLock attemptToObtainFile(FileChannel fileChannel) throws IOException {
      FileLock fileLock = null;
      while (fileLock == null) {
        try {
          fileLock = fileChannel.tryLock();
        }
        catch (OverlappingFileLockException e) { //a lock that overlaps the requested region is already held by this Java virtual machine
          //ignore and return null
        }
        catch (ClosedChannelException e) { //channel is closed
          throw new IOException("Channel Closed", e);
        }
        catch (IOException e) { //other io errors
          //try again
        }
      }
      return fileLock;
    }

  }
}
