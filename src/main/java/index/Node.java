package index;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.Vector;

public abstract class Node implements Serializable {
    //    int combinedIndex;
    Vector<Comparable> leftLimit, rightLimit, mid;
    Node parent;
    Integer indexInParent;

    // Function to get the string at
    // the middle of lexicographically
    // increasing sequence of strings from S to T
    // Method outsourced from GeeksForGeeks and modified, the link is: https://www.geeksforgeeks.org/print-middle-string-lexicographically-increasing-sequence-strings-s-t/
    public static String getMiddleString(String S, String T) {
        //s and t will both be of same lenth

        int N = S.length();
        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];


        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int) S.charAt(i) - 97
                    + (int) T.charAt(i) - 97;
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int) a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int) a1[i] / 2;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= N; i++) {
            sb.append((char) (a1[i] + 97));
        }
        return sb.toString();
    }

    public void setBounds(Vector<Comparable> lefts, Vector<Comparable> rights) {
        leftLimit = new Vector<>();
        rightLimit = new Vector<>();
        for (Comparable x : lefts) {
            leftLimit.add(x);
        }
        for (Comparable x : rights) {
            rightLimit.add(x);
        }
    }

    public void setMid(Vector<Comparable> minPerCol, Vector<Comparable> maxPerCol, Vector<String> typePerCol) throws IOException {
        this.mid = new Vector<>();
        for (int i = 0; i < typePerCol.size(); i++) {
            if (typePerCol.get(i).equals("java.lang.Integer")) {
                int l = (int) minPerCol.get(i);
                int r = (int) maxPerCol.get(i);
                int mid = l + ((r - l) >> 1);
                this.mid.add(mid);
            } else if (typePerCol.get(i).equals("java.lang.Double") || typePerCol.get(i).equals("java.lang.double")) {
                double l = (double) minPerCol.get(i);
                double r = (double) maxPerCol.get(i);
                double mid = l + ((r - l) / 2.0);
                this.mid.add(mid);
            } else if (typePerCol.get(i).equals("java.lang.String")) {
                String s = (String) minPerCol.get(i);
                String t = (String) maxPerCol.get(i);
                if (s.length() < t.length()) {
                    s += t.substring(s.length(), t.length());
                }
                this.mid.add(getMiddleString(s, t));
            } else if (typePerCol.get(i).equals("java.util.Date")) {
                Date min = (Date) minPerCol.get(i);
                Date max = (Date) maxPerCol.get(i);
                long mid = min.getTime() + (max.getTime() - min.getTime()) / 2;
                this.mid.add(new Date(mid));
            }
        }
    }

    public void set(Vector<Comparable> minPerCol, Vector<Comparable> maxPerCol, Vector<String> typePerCol) throws IOException {
        setBounds(minPerCol, maxPerCol);
        setMid(minPerCol, maxPerCol, typePerCol);
    }

    public Vector<Comparable> getMid() {
        return mid;
    }

    public Node getParent() {
        return parent;
    }

    public void setParent(Node newNode) {
        parent = newNode;
    }

    public Integer getIndexInParent() {
        return indexInParent;
    }

    public void setIndexInParent(Integer i) {
        indexInParent = i;
    }

    public String toString() {

        return "{" + " Left Limit: " + leftLimit + " Right Limit: " + rightLimit + " " + indexInParent;
    }

}
