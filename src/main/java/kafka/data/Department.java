package kafka.data;

public enum Department {
    COMPUTER_ENGINEERING("Computer Engineering"),
    CIVIL_ENGINEERING("Civil Engineering"),
    IT_ENGINEERING("Information Technology Engineering"),
    MECHANICAL_ENGINEERING("Mechanical Engineering");

    private final String departmentName;

    Department(String departmentName) {
        this.departmentName = departmentName;
    }

    public String getDepartmentName() {
        return departmentName;
    }
}
