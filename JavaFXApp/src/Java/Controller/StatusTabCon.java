package Java.Controller;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.Label;
import javafx.scene.layout.AnchorPane;
import javafx.scene.text.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

//todo add the start/stop scripts, need admin access
public class StatusTabCon {

    @FXML private Label displayText;
    @FXML private Text updateTimer;
    private Boolean threadStop = false;
    private final int updateFreq = 10;

    @FXML
    public void startHDFS(){
        System.out.println("HDFS Start");
        displayText.setText("starting");
    }

    @FXML
    public void stopHDFS(){
        System.out.println("HDFS stop");
        displayText.setText("terminating");
    }

    @FXML
    private void statusUpdate(){
        Thread t = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        while (!threadStop) {
                            int start = updateFreq;
                            try {
                                while (start!=0){
                                    int finalStart = start;
                                    Platform.runLater(()->{
                                        updateTimer.setText(String.valueOf(finalStart));
                                    });
                                    Thread.sleep(1000);
                                    start-=1;
                                }
                                ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/C", "hadoop dfsadmin -report");
                                Process p = builder.start();

                                BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
                                StringBuffer buffer = new StringBuffer();
                                String line = "";
                                while (true) {
                                    buffer.append(line).append("\n");
                                    line = r.readLine();
                                    if (line == null) {
                                        break;
                                    }
                                }
                                String message_ping = buffer.toString();
                                p.waitFor();
                                r.close();

                                Platform.runLater(new Runnable() {
                                    @Override
                                    public void run() {
                                        displayText.setText(message_ping);
                                    }
                                });
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
        );
        t.start();
    }

    @FXML
    public void initialize() throws IOException, InterruptedException {
        statusUpdate();
    }
}
