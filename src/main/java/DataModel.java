public class DataModel {
    private String ID;
    private String Equipment;

    public DataModel() {
    }

    public DataModel(String ID, String equipment) {
        this.ID = ID;
        Equipment = equipment;
    }

    @Override
    public String toString() {
        return "DataModel{" +
                "ID='" + ID + '\'' +
                ", Equipment='" + Equipment + '\'' +
                '}';
    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public String getEquipment() {
        return Equipment;
    }

    public void setEquipment(String equipment) {
        Equipment = equipment;
    }
}
