package com.github.zhongl.store.benchmark;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class RandomOptionalFactory implements CallableFactory {
    private final List<CallableFactory> options;
    private final Random random = new Random(1L); // use 1L for random the same sequance every time.


    public RandomOptionalFactory(CallableFactory... options) {
        this(((List<CallableFactory>) asList(options)));
    }

    private static List<CallableFactory> asList(CallableFactory[] options) {
        List<CallableFactory> list = new ArrayList<CallableFactory>();
        Collections.addAll(list, options);
        return list;
    }


    public RandomOptionalFactory(List<CallableFactory> options) {this.options = options;}


    @Override
    public Callable<?> create() {
        synchronized (options) {
            int index = Math.abs(random.nextInt()) % options.size();
            Callable<?> callable = options.get(index).create();
            if (callable != null) return callable;
            options.remove(index);
            if (options.isEmpty()) return null;
        }
        return create();
    }
}
