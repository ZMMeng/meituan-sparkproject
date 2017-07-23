package com.mzm.sparkproject.spark.session;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 品类二次排序Key
 * Created by 蒙卓明 on 2017/7/21.
 */
public class CategorySecondarySortKey implements Ordered<CategorySecondarySortKey>,Serializable {

    //点击次数
    private long clickCount;
    //下单次数
    private long orderCount;
    //支付次数
    private long payCount;

    public CategorySecondarySortKey() {
    }

    public CategorySecondarySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySecondarySortKey other) {

        if (this == other) {
            return 0;
        }

        int tmp = Long.compare(clickCount, other.clickCount);
        if (tmp != 0) {
            return tmp;
        }

        tmp = Long.compare(orderCount, other.orderCount);
        if (tmp != 0) {
            return tmp;
        }

        return Long.compare(payCount, other.payCount);
    }

    @Override
    public boolean $less(CategorySecondarySortKey other) {

        if (clickCount < other.clickCount) {
            return true;
        }

        if (clickCount == other.clickCount && orderCount < other.orderCount) {
            return true;
        }

        if (clickCount == other.clickCount && orderCount == other.orderCount && payCount < other.payCount) {
            return true;
        }

        return false;
    }

    @Override
    public boolean $greater(CategorySecondarySortKey other) {

        if (clickCount > other.clickCount) {
            return true;
        }

        if (clickCount == other.clickCount && orderCount > other.orderCount) {
            return true;
        }

        if (clickCount == other.clickCount && orderCount == other.orderCount && payCount > other.payCount) {
            return true;
        }

        return false;
    }

    @Override
    public boolean $less$eq(CategorySecondarySortKey other) {

        if ($less(other)) {
            return true;
        }

        if (clickCount == other.clickCount && orderCount == other.orderCount && payCount == other.payCount) {
            return true;
        }

        return false;
    }

    @Override
    public boolean $greater$eq(CategorySecondarySortKey other) {
        if ($greater(other)) {
            return true;
        }

        if (clickCount == other.clickCount && orderCount == other.orderCount && payCount == other.payCount) {
            return true;
        }

        return false;
    }

    @Override
    public int compareTo(CategorySecondarySortKey other) {
        return compare(other);
    }
}
