package Java.Helper;

public class WeightedTicker {
    private String ticker;
    private double weight;

    public WeightedTicker(){
        ticker="";
        weight=0.0;
    }

    public WeightedTicker(String _ticker, double _weight){
        ticker = _ticker;
        weight = _weight;
    }


    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }
}
