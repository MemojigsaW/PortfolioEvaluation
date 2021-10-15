package Java.Controller;

import Java.Interfaces.ControlMediator;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.text.Text;
import javafx.scene.text.TextAlignment;
import org.controlsfx.control.textfield.TextFields;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PortCon {

    private JSONObject config = null;
    private ArrayList<String> tickerlist = new ArrayList<>();
    private ObservableList<String> selectedTickers = FXCollections.observableArrayList();
    private ControlMediator parentListener;

    @FXML private TextField searchField;
    @FXML private Button addButton;
    @FXML private Button clsSelectedButton;
    @FXML private Button clsAllButton;
    @FXML private ListView<String> listView ;
    @FXML private Button submitButton;
    @FXML private Text loadingText;
    @FXML private ProgressIndicator loadingSpinner;
    @FXML private Text errorText;

    private EventHandler<ActionEvent> handlerAdd = new EventHandler<ActionEvent>() {
        @Override
        public void handle(ActionEvent event) {
            String val = searchField.getText();
            if (tickerlist.contains(val) && !selectedTickers.contains(val)){
                selectedTickers.add(val);
            }else{
                Alert errorAlert = new Alert(Alert.AlertType.ERROR);
                errorAlert.setHeaderText("Invalid Input");
                errorAlert.setContentText("Must be a valid ticker from dataset");
                errorAlert.showAndWait();
                searchField.requestFocus();
            }
        }
    };

    private EventHandler<ActionEvent> handlerClsSelected = new EventHandler<ActionEvent>() {
        @Override
        public void handle(ActionEvent event) {
            ObservableList<Integer> selectedIndices = listView.getSelectionModel().getSelectedIndices();
            for (Integer index: selectedIndices){
                selectedTickers.remove(index.intValue());
            }
        }
    };

    private EventHandler<ActionEvent> handlerClsAll = new EventHandler<ActionEvent>() {
        @Override
        public void handle(ActionEvent event) {
            selectedTickers.clear();
        }
    };

    private EventHandler<ActionEvent> handlerSubmit = new EventHandler<ActionEvent>() {
        @Override
        public void handle(ActionEvent event) {
            parentListener.parentListener();
        }
    };

    public void setConfig(JSONObject _config) {
        config = _config;
        updateUI();
    }

    public void setListener(ControlMediator _listener){
        parentListener = _listener;
    }

    private void updateUI() {
        JSONArray tickers = (JSONArray)config.get("allowedTickers");
        for (int i=0; i<tickers.size(); i++){
            tickerlist.add((String)tickers.get(i));
        }
        String[] tickers_str = tickerlist.toArray(new String[tickerlist.size()]);
        TextFields.bindAutoCompletion(searchField, tickers_str);
    }

    public String submitTickers(){
        List<String> str_list = new ArrayList<>(selectedTickers);
        if (str_list.isEmpty()){
            Alert errorAlert = new Alert(Alert.AlertType.ERROR);
            errorAlert.setHeaderText("No tickers are selected");
            errorAlert.setContentText("Minimum one ticker is needed");
            errorAlert.showAndWait();
            return "";
        }
        return String.join(" ", str_list);
    }

    public void setLoadingMode(boolean mode){
        if (mode){
//            in loading mode
            loadingText.setVisible(true);
            loadingSpinner.setVisible(true);
            submitButton.setDisable(true);
        }else{
//            out of loading mode
            loadingText.setVisible(false);
            loadingSpinner.setVisible(false);
            submitButton.setDisable(false);
        }

    }

    public void setLoadingMsg(String msg){
        loadingText.setText(msg);
    }

    public void setErrorMode(boolean mode){
        errorText.setVisible(mode);
    }

    public void setErrorText(String msg){
        errorText.setText(msg);
    }

    @FXML
    public void initialize(){
        addButton.setOnAction(handlerAdd);
        clsSelectedButton.setOnAction(handlerClsSelected);
        clsAllButton.setOnAction(handlerClsAll);
        listView.setItems(selectedTickers);
        listView.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
        submitButton.setOnAction(handlerSubmit);

        loadingText.setTextAlignment(TextAlignment.CENTER);

        setLoadingMode(false);
        setErrorMode(false);
    }
}
