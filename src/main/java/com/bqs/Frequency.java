package com.bqs;

import javafx.util.Pair;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.*;

public class Frequency  {
    static Map<String, Integer> frequencyMap = new HashMap<>();
    static Map<String, Long> intervalMap = new HashMap<>();

    private static  Comparator<Pair<Long, String>> cmp = (a, b) -> {
        if (a.getKey().equals(b.getKey())) return a.getValue().compareTo(b.getValue()); //第二键值做参考
        return (int) (a.getKey() - b.getKey()); //第一键值做参考
    };

    static Queue<Pair<Long, String>> queue = new PriorityQueue<>(cmp);
}
