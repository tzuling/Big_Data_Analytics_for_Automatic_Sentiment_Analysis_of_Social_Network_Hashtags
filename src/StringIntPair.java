
public class StringIntPair {
	
	private String string;
    private int integer;

    public StringIntPair(String s, int i) {
        string = s;
        integer = i;
    }

    protected String getString() {
        return string;
    }

    protected int getInteger() {
        return integer;
    }

    public String toString() {
        return string + "," + integer;
    }
    
}
