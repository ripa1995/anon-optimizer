package test;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class AllTests extends TestCase {

    public AllTests(String s) {
        super(s);
    }

    public static Test suite() {

        TestSuite suite = new TestSuite();

        suite.addTestSuite(test.TestUtilityExtractor.class);
        suite.addTestSuite(test.TestAnonymizer.class);

        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}

