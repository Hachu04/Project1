import org.junit.Test;

import static org.junit.Assert.*;

public class TaskCTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[2];

        /*
        1. put the data.txt into a folder in your pc
        2. add the path for the following two files.
            windows : update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            mac or linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

        input[0] = "file:///Users/hachu/Documents/Big Data/Project1/input/pages.csv";
        input[1] = "file:///Users/hachu/Documents/Big Data/Project1/output";

        TaskC taskC = new TaskC();
        taskC.debug(input);
    }
}