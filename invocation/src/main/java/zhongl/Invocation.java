package zhongl;

import com.google.caliper.SimpleBenchmark;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class Invocation extends SimpleBenchmark {

    private final Runnable target = new Runnable() {
        @Override
        public void run() {
            "hello".contains("a");
        }
    };

    private final Method run;

    public Invocation() throws NoSuchMethodException {
        run = Runnable.class.getDeclaredMethod("run");
    }

    public void timeDirectInvoke(int reps) {
        for (int i = 0; i < reps; i++) {
            target.run();
        }
    }

    public void timeReflectInvoke(int reps) throws InvocationTargetException, IllegalAccessException {
        for (int i = 0; i < reps; i++) {
            run.invoke(target);
        }
    }
}
