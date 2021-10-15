package Java.Controller;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Text;
import javafx.util.Callback;
import javafx.util.converter.IntegerStringConverter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class SettingCon {
    @FXML
    private DatePicker dateStart;
    @FXML
    private DatePicker dateEnd;
    @FXML
    private DatePicker dateMiddle;

    @FXML
    private Pane controlPane;

    @FXML private ComboBox<String> modelType;
    @FXML private TextField lambdaField;
    @FXML private CheckBox shortingCheck;
    @FXML private TextField normDField;
    @FXML private TextField normBoundField;
    @FXML private ComboBox<String> rmvoType;
    @FXML private CheckBox normCheck;
    @FXML private TextField confField;


    private JSONObject config = null;
    private LocalDate f_date = null;
    private LocalDate s_date = null;

    private EventHandler<ActionEvent> datePickerEvent = new EventHandler<ActionEvent>() {
        @Override
        public void handle(ActionEvent event) {
            restrictDates(dateEnd, dateStart.getValue(), f_date);
            restrictDates(dateStart, s_date, dateEnd.getValue());
            if (dateMiddle.getValue() != null && (dateMiddle.getValue().isBefore(dateStart.getValue()) || dateMiddle.getValue().isAfter(dateEnd.getValue()))) {
                dateMiddle.setValue(null);
            }
            restrictDates(dateMiddle, dateStart.getValue(), dateEnd.getValue());
        }
    };

    private EventHandler<ActionEvent> normCheckEvent = new EventHandler<ActionEvent>() {
        @Override
        public void handle(ActionEvent event) {
            if (normCheck.isSelected()){
                normDField.setDisable(false);
                normBoundField.setDisable(false);
            }else{
                normDField.setDisable(true);
                normBoundField.setDisable(true);
            }
        }
    };

    private EventHandler<ActionEvent> modelEvent = new EventHandler<ActionEvent>() {
        @Override
        public void handle(ActionEvent event) {
            String val = modelType.getValue();
            switch (val){
                case "MVO":
                    lambdaField.setDisable(false);
                    shortingCheck.setDisable(false);
                    normCheck.setDisable(false);
                    normCheck.fireEvent(new ActionEvent());
                    confField.setDisable(true);
                    rmvoType.setDisable(true);
                    break;
                case "RMVO":
                    lambdaField.setDisable(false);
                    shortingCheck.setDisable(false);
                    normCheck.setDisable(true);
                    normBoundField.setDisable(true);
                    normDField.setDisable(true);
                    confField.setDisable(false);
                    rmvoType.setDisable(false);
                    break;
                case "ERC":
                    lambdaField.setDisable(true);
                    shortingCheck.setSelected(true);
                    shortingCheck.setDisable(true);
                    normCheck.setDisable(true);
                    normBoundField.setDisable(true);
                    normDField.setDisable(true);
                    confField.setDisable(true);
                    rmvoType.setDisable(true);
                    break;
                default:
                    break;
            }
        }
    };

    private EventHandler<ActionEvent> confEvent = new EventHandler<ActionEvent>() {
        @Override
        public void handle(ActionEvent event) {
            if (confField.getText().equals("")){
                confField.setText("0.95");
                return;
            }else{
                double val = Double.parseDouble(confField.getText());
                if (val<=0){
                    confField.setText("0.01");
                    confField.requestFocus();
                }else if (val>=1){
                    confField.setText("0.99");
                    confField.requestFocus();
                }
            }
        }
    };

    public String submitConf() {
        boolean check = validateConf();
        if (!check){
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();

        DateTimeFormatter dateFormatter  = DateTimeFormatter.ofPattern("MM/01/yyyy");
        stringBuilder.append(dateStart.getValue().format(dateFormatter)).append(" ");
        stringBuilder.append(dateMiddle.getValue().format(dateFormatter)).append(" ");
        stringBuilder.append(dateEnd.getValue().format(dateFormatter)).append(" ");
        stringBuilder.append(modelType.getValue()).append(" ");
        stringBuilder.append(lambdaField.getText()).append(" ");
        String output = String.valueOf(shortingCheck.isSelected()).substring(0, 1).toUpperCase() + String.valueOf(shortingCheck.isSelected()).substring(1);
        stringBuilder.append(output).append(" ");
        String LN;
        if (normCheck.isSelected()){
            LN = normDField.getText();
        }else{
            LN = "-1";
        }
        stringBuilder.append(LN).append(" ");
        stringBuilder.append(normBoundField.getText()).append(" ");
        stringBuilder.append(rmvoType.getValue()).append(" ");
        stringBuilder.append(confField.getText());
        return stringBuilder.toString();
    }

    private boolean validateConf(){
        if (dateMiddle.getValue() ==null){
            Alert errorAlert = new Alert(Alert.AlertType.ERROR);
            errorAlert.setHeaderText("Missing training end date");
            errorAlert.setContentText("A training end date is required");
            errorAlert.showAndWait();
            return false;
        }
        return true;
    }

    public void setConfig(JSONObject _config) throws ParseException {
        config = _config;
        updateUI();
    }

    private void updateUI() throws ParseException {
//        date picker range
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/d/yyyy");

        LocalDate localMin = LocalDate.parse((String) config.get("min_date"), formatter);
        LocalDate localMax = LocalDate.parse((String) config.get("max_date"), formatter);

        restrictDates(dateStart, localMin, localMax);
        restrictDates(dateStart, localMin, localMax);
        restrictDates(dateMiddle, localMin, localMax);

        f_date = localMax;
        s_date = localMin;
//        combo boxes
        JSONArray modelOptions = (JSONArray)config.get("model_options");
        for (int i=0; i<modelOptions.size();i++){
            modelType.getItems().add((String)modelOptions.get(i));
        }

        JSONArray rmvoOptions = (JSONArray)config.get("rmvo_options");
        for (int i=0; i<rmvoOptions.size();i++){
            rmvoType.getItems().add((String)rmvoOptions.get(i));
        }
//        initialization
        dateStart.setValue(localMin);
        dateEnd.setValue(localMax);
        dateMiddle.setValue(localMax.minusYears(1));
        lambdaField.setText("1");
        normDField.setText("1");
        normBoundField.setText("1.5");
        modelType.getSelectionModel().select(0);
        rmvoType.getSelectionModel().select(0);
        confField.setText("0.95");
        normCheck.setSelected(false);
        modelType.fireEvent(new ActionEvent());
        normCheck.fireEvent(new ActionEvent());
    }


    private void restrictDates(DatePicker datePicker, LocalDate minDate, LocalDate maxDate) {
        final Callback<DatePicker, DateCell> dayCellFactory = new Callback<DatePicker, DateCell>() {
            @Override
            public DateCell call(final DatePicker datePicker) {
                return new DateCell() {
                    @Override
                    public void updateItem(LocalDate item, boolean empty) {
                        super.updateItem(item, empty);
                        if (item.isBefore(minDate)) {
                            setDisable(true);
                            setStyle("-fx-background-color: #ffc0cb;");
                        } else if (item.isAfter(maxDate)) {
                            setDisable(true);
                            setStyle("-fx-background-color: #ffc0cb;");
                        }
                    }
                };
            }
        };
        datePicker.setDayCellFactory(dayCellFactory);
    }





    @FXML
    public void initialize() {
        dateStart.setOnAction(datePickerEvent);
        dateEnd.setOnAction(datePickerEvent);
        dateMiddle.setOnAction(datePickerEvent);
        controlPane.setBorder(new Border(new BorderStroke(Color.BLACK,
                BorderStrokeStyle.SOLID, CornerRadii.EMPTY, BorderWidths.DEFAULT)));
        DecimalFormat dFormat = new DecimalFormat("#.0");

        normDField.setTextFormatter(new TextFormatter<>(new IntegerStringConverter()));

        dFormatter(dFormat, lambdaField);
        dFormatter(dFormat, normBoundField);
        dFormatter(dFormat, confField);

        normCheck.setOnAction(normCheckEvent);
        modelType.setOnAction(modelEvent);
        confField.setOnAction(confEvent);
        confField.focusedProperty().addListener(new ChangeListener<Boolean>() {
            @Override
            public void changed(ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) {
                if (newValue){
//                    in focus
                }else{
//                    out of focus
                    confField.fireEvent(new ActionEvent());
                }
            }
        });

        setOutofFocusValue(lambdaField);
        setOutofFocusValue(normDField);
        setOutofFocusValue(normBoundField);
    }

    private void setOutofFocusValue(TextField textField){
        textField.focusedProperty().addListener(new ChangeListener<Boolean>() {
            @Override
            public void changed(ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) {
                if (newValue){

                }else{
                    if (textField.getText().equals("")){
                        textField.setText("1");
                    }
                }
            }
        });

    }

    private void dFormatter(DecimalFormat dFormat, TextField lambdaField) {
        lambdaField.setTextFormatter( new TextFormatter<>(c ->
        {
            if ( c.getControlNewText().isEmpty() )
            {
                return c;
            }

            ParsePosition parsePosition = new ParsePosition( 0 );
            Object object = dFormat.parse( c.getControlNewText(), parsePosition );

            if ( object == null || parsePosition.getIndex() < c.getControlNewText().length() )
            {
                return null;
            }
            else
            {
                return c;
            }
    }));
    }
}



