package Java.Helper;

import Java.Interfaces.GlobalMediator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;

public class ConfigObj implements GlobalMediator {
    private JSONObject config = null;

    @Override
    public void setParameters(String... parameters) {
        loadConfig(parameters[0]);
    }

    @Override
    public  void getParameters() {
    }

    public JSONObject getConfig(){
        return config;
    }

    private void loadConfig(String config_path){
        JSONParser parser = new JSONParser();
        try{
            Object object = parser.parse(new FileReader(config_path));
            config = (JSONObject) object;
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private ConfigObj(){}

    public static ConfigObj getInstance(){
        return ConfigObjMediator.INSTANCE;
    }

    public static class ConfigObjMediator {
        private static final ConfigObj INSTANCE = new ConfigObj();
    }
}
