package org.apache.kafka.clients.admin.test;

public class ThisEscape2 {

    public ThisEscape2(ThisEscape thisEscape) {
    }

    public void escape(ThisEscape thisEscape) {
        System.out.println(thisEscape);
    }

    public void escape(ThisEscape2 thisEscape) {
        System.out.println(thisEscape);
    }

    public void escape(String thisEscape) {
        System.out.println(thisEscape);
    }

}
