package cn.edu.fudan.dsm.basic.common;

import cn.edu.fudan.dsm.basic.common.entity.IndexNode;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by huibo on 2016/12/7.
 */
public class IndexCache {

    private String beginRound;

    private String endRound;

    private NavigableMap<String, IndexNode> caches;

    public IndexCache(String beginRound, String endRound) {
        this.beginRound = beginRound;
        this.endRound = endRound;
        caches = new ConcurrentSkipListMap<>();
    }

    public IndexCache(String beginRound, String endRound, NavigableMap<String, IndexNode> caches) {
        this.beginRound = beginRound;
        this.endRound = endRound;
        this.caches = caches;
    }

    public void addCache(String meanRound, IndexNode indexNode) {
        caches.put(meanRound, indexNode);
    }

    public String getBeginRound() {
        return beginRound;
    }

    public void setBeginRound(String beginRound) {
        this.beginRound = beginRound;
    }

    public String getEndRound() {
        return endRound;
    }

    public void setEndRound(String endRound) {
        this.endRound = endRound;
    }

    public NavigableMap<String, IndexNode> getCaches() {
        return caches;
    }

    public void setCaches(NavigableMap<String, IndexNode> caches) {
        this.caches = caches;
    }
}
