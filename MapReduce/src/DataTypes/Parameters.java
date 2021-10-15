package DataTypes;

import java.util.ArrayList;
import java.util.Date;

public class Parameters {
    private ArrayList<Integer> selectedIndices;
    private Date startDate, midDate, testDate;

    public ArrayList<Integer> getSelectedIndices() {
        return selectedIndices;
    }

    public void setSelectedIndices(ArrayList<Integer> selectedIndices) {
        this.selectedIndices = selectedIndices;
    }


    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getMidDate() {
        return midDate;
    }

    public void setMidDate(Date midDate) {
        this.midDate = midDate;
    }

    public Date getTestDate() {
        return testDate;
    }

    public void setTestDate(Date testDate) {
        this.testDate = testDate;
    }
}
