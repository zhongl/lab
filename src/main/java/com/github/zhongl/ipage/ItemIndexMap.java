package com.github.zhongl.ipage;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface ItemIndexMap {
    ItemIndex put(Md5Key key, ItemIndex itemIndex);

    ItemIndex get(Md5Key key);

    ItemIndex remove(Md5Key key);
}
