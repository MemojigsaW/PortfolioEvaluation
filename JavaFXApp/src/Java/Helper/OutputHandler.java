package Java.Helper;

import Java.Main;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

//method to set process output stream to system
//for debugging purposes only
//https://stackoverflow.com/questions/48822046/how-to-know-if-my-processbuilder-start-executed-the-commands-successfully
public class OutputHandler extends Thread {
    private final StringBuilder buf = new StringBuilder();
    private final BufferedReader in;

    public OutputHandler(InputStream in, String encoding)
            throws UnsupportedEncodingException {
        this.in = new BufferedReader(new InputStreamReader(
                in, encoding == null ? "UTF-8" : encoding));
        setDaemon(true);
        start();
    }

    public String getText() {
        synchronized(buf) {
            return buf.toString();
        }
    }

    @Override
    public void run() {
        // Reading process output
        try {
            String s = in.readLine();
            while (s != null) {
                synchronized(buf) {
                    buf.append(s);
                    buf.append('\n');
                }
                s = in.readLine();
            }
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
