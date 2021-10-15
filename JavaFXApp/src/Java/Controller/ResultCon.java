package Java.Controller;

import Java.Helper.ConfigObj;
import Java.Helper.WeightedTicker;
import Java.Interfaces.ControlMediator;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.XYChart;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class ResultCon {
    @FXML
    private TableView<WeightedTicker> tickerTable;
    @FXML
    private TableColumn<WeightedTicker, String> tickerColumn;
    @FXML
    private TableColumn<WeightedTicker, Double> weightColumn;
    @FXML
    private LineChart<String, Number> lineChart;

    private ObservableList<WeightedTicker> tickerList;

    private JSONObject config;
    private ControlMediator parentListener;

    public void setTickerList(String _tickers, String _weights) {
        tickerList.clear();
        String[] tickers = _tickers.split(" ");
        String[] weights = _weights.split(",");
        for (int i = 0; i < tickers.length; i++) {
            tickerList.add(new WeightedTicker(tickers[i], Double.parseDouble(weights[i])));
        }
        setGraph();
    }

    public void setConfig(JSONObject _config) {
        config = _config;
    }

    public void setListener(ControlMediator _listener){
        parentListener = _listener;
    }

    private void setGraph() {
        Platform.runLater(new Runnable() {
            @Override
            public void run() {
                lineChart.getData().clear();
                XYChart.Series<String, Number> series = new XYChart.Series<>();
                String filepath = config.get("file_Loc") + "/testplot.csv";
                BufferedReader csvReader = null;
                try {
                    String row = null;
                    csvReader = new BufferedReader(new FileReader(filepath));
                    while ((row = csvReader.readLine()) != null) {
                        String[] data = row.split(",");
                        series.getData().add(new XYChart.Data<String, Number>(data[0], Double.parseDouble(data[1])));
                    }
                    csvReader.close();
                    finishUpdate(0);
                } catch (Exception e) {
                    e.printStackTrace();
                    finishUpdate(-1);
                }
                lineChart.getData().add(series);
            }
        });
    }

    private void finishUpdate(int result) {
        parentListener.parentListener(result);
    }

    @FXML
    public void initialize() {
        tickerList = FXCollections.observableArrayList();
        tickerColumn.setCellValueFactory(new PropertyValueFactory<>("ticker"));
        weightColumn.setCellValueFactory(new PropertyValueFactory<>("weight"));
        tickerTable.setItems(tickerList);
        lineChart.setLegendVisible(false);
    }
}
