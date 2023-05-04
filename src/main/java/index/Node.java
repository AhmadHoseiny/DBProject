package index;

import java.util.Date;
import java.util.Vector;

public abstract class Node {
//    int combinedIndex;
    Vector<Comparable> leftLimit, rightLimit, mid;
    Node parent;
    Integer indexInParent;


    public void setBounds(Vector<Comparable> lefts,Vector<Comparable> rights) {
        leftLimit = new Vector<>();
        rightLimit = new Vector<>();
        for(Comparable x : lefts){
            leftLimit.add(x);
        }
        for (Comparable x : rights){
            rightLimit.add(x);
        }
    }

//    public boolean isInside(Comparable x, Comparable y, Comparable z) {
//        return x.compareTo(leftLimit.get(0)) >= 0 && x.compareTo(rightLimit.get(0)) <= 0 &&
//                y.compareTo(leftLimit.get(1)) >= 0 && y.compareTo(rightLimit.get(1)) <= 0 &&
//                z.compareTo(leftLimit.get(2)) >= 0 && z.compareTo(rightLimit.get(2)) <= 0;
//    }


    public void setMid (Vector<Comparable> minPerCol, Vector<Comparable> maxPerCol, Vector<String> typePerCol) {

        for (int i = 0; i < typePerCol.size(); i++) {
            if (typePerCol.get(i).equals("java.lang.Integer")) {
                int l = (int) minPerCol.get(i);
                int r = (int) maxPerCol.get(i);
                int mid = l + ((r-l)>>1);
                this.mid.add(mid);
            }
            else if (typePerCol.get(i).equals("java.lang.Double")) {
                double l = (double) minPerCol.get(i);
                double r = (double) maxPerCol.get(i);
                double mid = l + ((r-l)/2.0);
                this.mid.add(mid);
            }
            else if (typePerCol.get(i).equals("java.lang.String")) {
                String s = (String) minPerCol.get(i);
                String t = (String) maxPerCol.get(i);
                char le[] = s.toCharArray();
                char ri[] = t.toCharArray();
                int diffLen = ri.length - le.length;
                int incLen = (1+diffLen)>>1;
                char mid[] = new char [le.length + incLen];
                for(int j=0 ; j<mid.length ; j++){
                    char leCh;
                    if(j>=le.length){
                        leCh = 'A';
                    }
                    else{
                        leCh = le[j];
                    }
                    mid[j] = (char)( ((int)leCh + (int)ri[j])>>1);
                }
                this.mid.add(new String(mid));
            }
            else if (typePerCol.get(i).equals("java.util.Date")) {
                Date min = (Date) minPerCol.get(i);
                Date max = (Date) maxPerCol.get(i);
                long mid = min.getTime() + (max.getTime() - min.getTime()) / 2;
                this.mid.add(new Date(mid));
            }
        }
    }

    public void set (Vector<Comparable> minPerCol, Vector<Comparable> maxPerCol, Vector<String> typePerCol) {
        setBounds(minPerCol, maxPerCol);
        setMid(minPerCol, maxPerCol, typePerCol);
    }

    public Vector<Comparable> getMid() {
        return mid;
    }


    public void setParent(Node newNode) {
        parent = newNode;
    }

    public Node getParent() {
        return parent;
    }

    public void setIndexInParent(Integer i) {
        indexInParent = i;
    }

    public Integer getIndexInParent() {
        return indexInParent;
    }
}
