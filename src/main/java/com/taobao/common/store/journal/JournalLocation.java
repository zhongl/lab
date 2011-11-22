package com.taobao.common.store.journal;

import java.io.Serializable;


/**
 * store4j 日志存储checkPoint的位置
 *
 * @author boyan(boyan@taobao.com)，shuihan
 * @date 2011-8-22
 *
 */
public final class JournalLocation implements Comparable<JournalLocation>, Serializable {
    public int number;
    public long offset;
    static final long serialVersionUID = -1L;


    public JournalLocation() {
        super();
    }


    public JournalLocation(final int number, final long offset) {
        super();
        this.number = number;
        this.offset = offset;
    }


    public int getNumber() {
        return this.number;
    }


    public void setNumber(final int number) {
        this.number = number;
    }


    public long getOffset() {
        return this.offset;
    }


    public void setOffset(final long offset) {
        this.offset = offset;
    }


    public int compareTo(final JournalLocation o) {
        final int rt = this.number - o.number;
        if (rt != 0) {
            return rt;
        }
        else {
            if (this.offset > o.offset) {
                return 1;
            }
            else if (this.offset < o.offset) {
                return -1;
            }
            else {
                return 0;
            }
        }

    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.number;
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final JournalLocation other = (JournalLocation) obj;
        if (this.number != other.number) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        return true;
    }

}
