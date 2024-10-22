package org.apache.kafka.clients.admin.test;

public class ThisEscape {

    private final ThisEscape2 thisEscape2;

    public ThisEscape() {
        this.thisEscape2 = new ThisEscape2(this);
        escape(thisEscape2);
    }

    public final void escape(ThisEscape2 thisEscape2) {
        thisEscape2.escape(thisEscape2);
    }

    public static void main(String[] args) {
        ThisEscape thisEscape = new ThisEscape();
        System.out.println(thisEscape);
    }
}
