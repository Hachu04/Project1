import org.junit.Test;

import static org.junit.Assert.*;

public class TaskATest {

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

        TaskA taskA = new TaskA();
        taskA.debug(input);
    }
}