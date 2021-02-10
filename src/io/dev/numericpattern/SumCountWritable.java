package io.dev.numericpattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class SumCountWritable implements WritableComparable<SumCountWritable> {

    private DoubleWritable sum;
    private DoubleWritable count;

    private  void set(DoubleWritable sum,DoubleWritable count){
        this.sum = sum;
        this.count = count;
    }

    public SumCountWritable(DoubleWritable sum, DoubleWritable count) {
        set(sum,count);
    }

    public SumCountWritable(Double sum,Double count){
        set(new DoubleWritable(sum),new DoubleWritable(count));
    }

    public SumCountWritable() {
        set(new DoubleWritable(),new DoubleWritable());
    }

    public DoubleWritable getSum() {
        return sum;
    }

    public void setSum(DoubleWritable sum) {
        this.sum = sum;
    }

    public DoubleWritable getCount() {
        return count;
    }

    public void setCount(DoubleWritable count) {
        this.count = count;
    }

    @Override
    public int compareTo(SumCountWritable sumCountWritable) {
        int compareResult = this.getSum().compareTo(sumCountWritable.getSum());
        if(compareResult!=0)
            return compareResult;
        return this.getCount().compareTo(sumCountWritable.getCount());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        sum.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sum.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SumCountWritable that = (SumCountWritable) o;
        return Objects.equals(sum, that.sum) && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, count);
    }

    @Override
    public String toString() {
        return "SumCountWritable{" +
                "sum=" + sum.toString() +
                ", count=" + count.toString() +
                '}';
    }
}
