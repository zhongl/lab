package com.github.zhongl.store.benchmark;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class RandomOptionalFactory<T> implements Factory<T> {
    private final List<Factory<T>> options;
    private final Random random = new Random(1L); // use 1L for random the same sequance every time.


    public RandomOptionalFactory(Factory<T>... options) {this(((List<Factory<T>>) asList(options)));}

    private static <T> List<Factory<T>> asList(Factory<T>[] options) {
        List<Factory<T>> list = new ArrayList<Factory<T>>();
        for (Factory<T> option : options) {
            list.add(option);
        }
        return list;
    }

    public RandomOptionalFactory(List<Factory<T>> options) {this.options = options;}

    @Override
    public T create() {
        synchronized (options) {
            int index = Math.abs(random.nextInt()) % options.size();
            T t = options.get(index).create();
            if (t != null) return t;
            options.remove(index);
            if (options.isEmpty()) return null;
        }
        return create();
    }
}
