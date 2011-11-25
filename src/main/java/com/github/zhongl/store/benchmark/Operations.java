package com.github.zhongl.store.benchmark;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Operations {

    private final int add;
    private final int get;
    private final int update;
    private final int remove;


    public Operations(int add, int get, int update, int remove) {
        this.add = add;
        this.get = get;
        this.update = update;
        this.remove = remove;
    }

    public int total() {
        return add + get + update + remove;
    }

    public boolean addRemainsOrCountDown() {
        return false;  // TODO addRemainsOrCountDown
    }
}
