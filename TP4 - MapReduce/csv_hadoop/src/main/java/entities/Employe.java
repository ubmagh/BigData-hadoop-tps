package entities;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Employe implements Writable {

    private String lname;
    private String fname;
    private String departement;
    private String job;
    private double salary;

    public Employe() {
    }

    public Employe(String lname, String fname, String departement, String job, double salary) {
        this.lname = lname;
        this.fname = fname;
        this.departement = departement;
        this.job = job;
        this.salary = salary;
    }

    public String getLname() {
        return lname;
    }

    public void setLname(String lname) {
        this.lname = lname;
    }

    public String getFname() {
        return fname;
    }

    public void setFname(String fname) {
        this.fname = fname;
    }

    public String getDepartement() {
        return departement;
    }

    public void setDepartement(String departement) {
        this.departement = departement;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Employe employe = (Employe) o;
        return Double.compare(employe.salary, salary) == 0 && Objects.equals(lname, employe.lname) && Objects.equals(fname, employe.fname) && Objects.equals(departement, employe.departement) && Objects.equals(job, employe.job);
    }


    @Override
    public int hashCode() {
        return Objects.hash(lname, fname, departement, job, salary);
    }

    @Override
    public String toString() {
        return "{" +
                " lname='" + lname + '\'' +
                ", fname='" + fname + '\'' +
                ", departement='" + departement + '\'' +
                ", job='" + job + '\'' +
                ", salary=" + salary +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // no need
        dataOutput.writeUTF(lname);
        dataOutput.writeUTF(fname);
        dataOutput.writeUTF(departement);
        dataOutput.writeUTF(job);
        dataOutput.writeDouble(salary);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        lname = dataInput.readUTF();
        fname = dataInput.readUTF();
        departement = dataInput.readUTF();
        job = dataInput.readUTF();
        salary = dataInput.readDouble();
    }
}
