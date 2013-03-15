package edu.uci.ics.hyracks.algebricks.core.rewriter.base;

/*
 * this class is static as independant of TagOperators2MappersOrReducers
 * there's an TaggingMRCounter annotation associated to an operator
 */

public final class TaggingMRCounter {

	public static String tagMapperReducerKey = "tagMapperReducerKey";
	public static String tagSuperNodeKey = "tagSuperNodeKey";

	String label;
    int counter;

    public TaggingMRCounter() {
        label        = "M";
        this.counter =  0;
    }

    public TaggingMRCounter(String label, int counter) {
        this.label   = label;
        this.counter = counter;
    }

    public int getCounter() {
        return counter;
    }

    public String getLabel() {
        return label;
    }

    public void incrementAndSetCounter() {
        this.counter++;
    }
    @Override
    public String toString() {

        return new String(label+counter);
    }
}
