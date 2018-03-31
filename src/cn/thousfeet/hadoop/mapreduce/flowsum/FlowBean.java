package cn.thousfeet.hadoop.mapreduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 需在结点间传递，那么需要遵循hadoop的序列化机制
 * @author thousfeet
 *
 */
public class FlowBean implements WritableComparable<FlowBean>{

	private String phoneNum;
	private long upFlow;
	private long downFlow;
	private long sumFlow;


	//将对象数据序列化到流中
	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(phoneNum);
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
		
	}

	//从数据流中反序列出对象的数据（必须跟序列化时的顺序保持一致）
	@Override
	public void readFields(DataInput in) throws IOException {
		
		phoneNum = in.readUTF();
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
		
	}
	
	//但在反序列化时，反射机制需要调用空参的默认构造函数
	public FlowBean() {};
	
	//为初始化方便加入一个带参构造函数
	public FlowBean(String phoneNum, long upFlow, long downFlow) {
		this.phoneNum = phoneNum;
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	//在reducer要write到context的时候需要调用toString，若没有重载那么会输出的是对象的ID
	@Override
	public String toString() {

		return " " + upFlow + "\t" + downFlow + "\t" + sumFlow;
		
	}
	
	//默认是比它大返回1，比它小返回-1，一样大返回0
	@Override
	public int compareTo(FlowBean o) {
		//将流量较大的排前面
		return sumFlow > o.getSumFlow() ? -1 : 1;
	}
	
	public String getPhoneNum() {
		return phoneNum;
	}

	public void setPhoneNum(String phoneNum) {
		this.phoneNum = phoneNum;
	}

	public long getupFlow() {
		return upFlow;
	}

	public void setupFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

}
