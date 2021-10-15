package Java.Controller;

import Java.Helper.ConfigObj;
import Java.Helper.OutputHandler;
import Java.Interfaces.ControlMediator;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Tab;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.text.ParseException;

public class MainCon {
    private String configPath_str = "C:\\Users\\Alan\\Desktop\\Projects\\PortfolioEval\\config.json";
    private String tickers = null;
    private String conf = null;
    private String dateSubstring = null;
    private JSONObject config;
    private SettingCon settingController;
    private PortCon portfolioController;
    private ResultCon resultController;

    private boolean mrQueryComplete = true;
    private int mrQueryStatus = 0;
    private int pyQueryStatus = 0;
    private boolean pyQueryComplete = true;
    private boolean resultComplete = true;
    private int resultStatus = 0;

    @FXML
    Tab statusTab, portfolioTab, settingTab, resultTab;

    private void updateTabs() throws IOException, ParseException {
        FXMLLoader settingLoader = new FXMLLoader(getClass().getResource("/Java/Resources/settingTab.fxml"));
        Parent settingRoot = settingLoader.load();
        settingController = settingLoader.getController();
        settingController.setConfig(config);
        settingTab.setContent((Node) settingRoot);

        FXMLLoader weightLoader = new FXMLLoader(getClass().getResource("/Java/Resources/weightingTab.fxml"));
        Parent weightRoot = weightLoader.load();
        portfolioController = weightLoader.getController();
        portfolioController.setConfig(config);
        portfolioController.setListener(new ControlMediator() {
            @Override
            public void parentListener() {
                submitTabs();
            }

            @Override
            public void parentListener(int i) {
                ;
            }
        });
        portfolioTab.setContent((Node) weightRoot);

        FXMLLoader resultLoader = new FXMLLoader(getClass().getResource("/Java/Resources/resultTab.fxml"));
        Parent resultRoot = resultLoader.load();
        resultController = resultLoader.getController();
        resultController.setConfig(config);
        resultController.setListener(new ControlMediator() {
            @Override
            public void parentListener() {
                ;
            }

            @Override
            public void parentListener(int i) {
                setResult(i);
            }
        });
        resultTab.setContent((Node)resultRoot);
    }

    private void submitTabs() {
        tickers = portfolioController.submitTickers();
        conf = settingController.submitConf();
        if (tickers.equals("") || conf.equals("")) {
            return;
        }
        dateSubstring = conf.substring(0, 32);
        System.out.println(dateSubstring);

        mrQuery();
        checkQueryResult();
    }

    private void mrQuery() {
        StringBuilder cmd_builder = new StringBuilder();
        cmd_builder.append("hadoop jar").append(" ");
        cmd_builder.append(config.get("MRjar")).append(" ");
        cmd_builder.append(configPath_str).append(" ");
        cmd_builder.append(dateSubstring).append(" ");
        cmd_builder.append(tickers);

        mrQueryComplete = false;
        mrQueryStatus = -1;
        pyQueryComplete = false;
        pyQueryStatus = -1;
        resultComplete = false;
        resultStatus = -1;

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/C", cmd_builder.toString());
                try {
                    System.out.println(cmd_builder.toString());
                    Process p = builder.start();
                    OutputHandler out
                            = new OutputHandler(p.getInputStream(), "UTF-8");
                    OutputHandler err
                            = new OutputHandler(p.getErrorStream(), "UTF-8");
                    mrQueryStatus = p.waitFor();
                    System.out.println("Status: " + mrQueryStatus);
                    out.join();
                    System.out.println("Output:");
                    System.out.println(out.getText());
                    System.out.println();
                    err.join();
                    System.out.println("Error:");
                    System.out.println(err.getText());
                    System.out.print(mrQueryStatus);
                    if (mrQueryStatus == 0) {
                        pyQuery();
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    mrQueryComplete = true;
                }
            }
        });
        t.start();
    }

    private void pyQuery() {
        StringBuilder cmdBuilder = new StringBuilder();
        cmdBuilder.append(config.get("PYenv")).append(" ");
        cmdBuilder.append("-u").append(" ");
        cmdBuilder.append(config.get("PYScript")).append("\\main.py").append(" ");
        cmdBuilder.append(configPath_str).append(" ");
        cmdBuilder.append(conf).append(" ");
        cmdBuilder.append(tickers);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                ProcessBuilder builder = new ProcessBuilder(cmdBuilder.toString().split(" "));
                try {
                    System.out.println(cmdBuilder.toString());
                    Process p = builder.start();
                    OutputHandler out
                            = new OutputHandler(p.getInputStream(), "UTF-8");
                    OutputHandler err
                            = new OutputHandler(p.getErrorStream(), "UTF-8");
                    pyQueryStatus = p.waitFor();
                    System.out.println("Status: " + pyQueryStatus);
                    out.join();
                    System.out.println("Output:");
                    System.out.println(out.getText());
                    System.out.println();
                    err.join();
                    System.out.println("Error:");
                    System.out.println(err.getText());
                    System.out.print(pyQueryStatus);
                    if (pyQueryStatus == 0) {
                        updateResult(out.getText());
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    pyQueryComplete = true;
                }
            }
        });
        t.start();
    }

    private void setResult(int mode){
        resultStatus = mode;
        resultComplete = true;
    }

    private void updateResult(String weights) {
        resultController.setTickerList(tickers, weights);
    }

    private void checkQueryResult() {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!mrQueryComplete) {
                    portfolioController.setLoadingMsg("Performing Map Reduce");
                    portfolioController.setLoadingMode(true);
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                if (mrQueryStatus != 0) {
                    portfolioController.setErrorText("Map Reduce failed");
                    portfolioController.setErrorMode(true);

                } else {
                    while (!pyQueryComplete) {
                        portfolioController.setLoadingMsg("Performing convex optimization");
                        try {
                            Thread.sleep(1000);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    if (pyQueryStatus != 0) {
                        portfolioController.setErrorText("Cvx Opt failed");
                        portfolioController.setErrorMode(true);
                    }else{
                        while (!resultComplete){
                            portfolioController.setLoadingMsg("Loading Results");
                            try{
                                Thread.sleep(1000);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                        if (resultStatus!=0){
                            portfolioController.setErrorText("Load Result Failed");
                            portfolioController.setErrorMode(true);
                        }
                    }
                }
                portfolioController.setLoadingMode(false);
            }
        });
        t.start();
    }

    @FXML
    public void initialize() throws IOException, ParseException {
        ConfigObj obj = ConfigObj.getInstance();
        obj.setParameters(configPath_str);
        config = obj.getConfig();
        if (config == null) {
            System.out.println("Config not found");
            System.exit(1);
        }
        updateTabs();
    }
}
