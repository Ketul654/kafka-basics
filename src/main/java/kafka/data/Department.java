package kafka.data;

public enum Department {
    HR("Human Resource"),
    IT("Information Technology"),
    FINANCE("Finance"),
    RECOVERY("Recovery"),
    LOAN("Loan");

    private final String departmentName;

    Department(String departmentName) {
        this.departmentName = departmentName;
    }

    public String getDepartmentName() {
        return departmentName;
    }
}
