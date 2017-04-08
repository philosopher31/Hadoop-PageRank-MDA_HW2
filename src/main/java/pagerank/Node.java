package pagerank;
import java.util.ArrayList;
import java.lang.Integer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.StringBuilder;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class Node implements WritableComparable {

	static final int OUT = 0;
	static final int IN = 1;
	private int type;
    private double pageRank;
    private int pageNode;
    private ArrayList<Integer> outlinks;
    public Node() {
    	outlinks = new ArrayList<Integer>();
    }
    
    public Node(int pageNode, double pageRank, ArrayList<Integer> outlinks) {
    	this.pageRank = pageRank;
    	this.pageNode = pageNode;
    	this.outlinks = outlinks;
    	this.type = OUT;
    }
    public Node(int pageNode, double pageRank) {
        this.pageRank = pageRank;
        this.pageNode = pageNode;
        this.outlinks = new ArrayList<Integer>();;
        this.type = OUT;
    }
    public Node(double pageRank) {
    	outlinks = new ArrayList<Integer>();
    	this.pageRank = pageRank;
    	this.type = IN;
    }

    @Override 
    public void write(DataOutput out) throws IOException {
    	out.writeInt(pageNode);
    	out.writeDouble(pageRank);
    	out.writeInt(type);
        out.writeInt(outlinks.size());
        for(int link:outlinks){
           out.writeInt(link);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pageNode = in.readInt();
        pageRank = in.readDouble();
        type = in.readInt();
        int size = in.readInt();
        outlinks.clear();
        for(int i=0;i<size;i++){
            outlinks.add(in.readInt());
        }
    }
    public void setPageNode(int pageNode){
    	this.pageNode = pageNode;

    }
    public int getPageNode(){
    	return pageNode;
    }

    public void setPageRank(double pageRank){
    	this.pageRank = pageRank;
    }
    public double getPageRank(){
    	return pageRank;
    }
    public int getType(){
    	return type;
    }
    @Override
    public String toString(){
    	StringBuilder s=new StringBuilder();
    	s.append(pageNode);
    	s.append("|");
    	s.append(pageRank);
    	s.append("|");
    	for(int link:outlinks){
    		s.append(link);
    		s.append(",");
    	}
    	if (s.charAt(s.length()-1) == ',')
    		s.deleteCharAt(s.length()-1);
    	return s.toString();
    }
    @Override
    public int compareTo(Object o) {
        Node o1 = (Node)o;
        int cmp = Double.compare(this.pageRank, o1.getPageRank());
        if(cmp == 0)
            return this.pageNode - o1.getPageNode();
        else
            return -cmp;
    }
}
