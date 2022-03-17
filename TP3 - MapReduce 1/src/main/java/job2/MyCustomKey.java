package job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyCustomKey implements WritableComparable<MyCustomKey> {

    private String city ;
    private int year;

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        city = in.readUTF();
        year = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(city);
        out.writeInt(year);
    }


    @Override
    public int compareTo(MyCustomKey myCustomKey) {
        int citiesComparaison = this.city.compareTo( myCustomKey.getCity() );

        // compare firstly with cities names
        if( citiesComparaison!= 0)
            return citiesComparaison;

        // if they have both same name, Then compare with year :
        if( this.year == myCustomKey.getYear() )
            return 0;
        if( this.year< myCustomKey.getYear())
            return -1;
        return 1;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 17 + year;
        hash = hash * 31 + city.hashCode()*( 1 + year>>>32 );
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyCustomKey that = (MyCustomKey) o;
        return year == that.year && city.equals(that.city);
    }
}
