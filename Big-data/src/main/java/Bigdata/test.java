package Bigdata;

import java.util.ArrayList;

public class test {
    public static void main(String[] args) {
        BatchView v=new BatchView();
         ArrayList<serviceResult> n=v.analysis("/user/hiberstack/messages/2022-03-24");
        for (serviceResult m:n) {
            System.out.println(m.toString());
        }
    }
}
