package base;

import org.junit.After;
import org.junit.Before;

import java.io.File;

/***
 * @author Dr. Chen
 * @version 1.0
 */
public class TestBase {
    @Before
    @After
    public void after() {
        (new File("redo")).delete();
        (new File("undo")).delete();
    }
}
