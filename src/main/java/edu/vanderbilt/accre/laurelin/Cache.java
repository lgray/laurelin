package edu.vanderbilt.accre.laurelin;

import java.lang.ref.SoftReference;
import java.util.WeakHashMap;

import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFile;

public class Cache {
    WeakHashMap<ROOTFile, WeakHashMap<String, WeakHashMap<Integer, SoftReference<RawArray>>>> cache;

    public Cache() {
        cache = new WeakHashMap<ROOTFile, WeakHashMap<String, WeakHashMap<Integer, SoftReference<RawArray>>>>();
    }

    public RawArray get(ROOTFile backingFile, String branch, int last) {
	WeakHashMap<String, WeakHashMap<Integer, SoftReference<RawArray>>> branchMap = cache.get(backingFile);
	if(branchMap == null) {
	    return null;
	}
        WeakHashMap<Integer, SoftReference<RawArray>> fileMap = branchMap.get(branch);
        if (fileMap == null) {
            return null;
        }
        SoftReference<RawArray> ref = fileMap.get(last);
        if (ref == null) {
            return null;
        }
        return ref.get();
    }

    public RawArray put(ROOTFile backingFile, String branch, int last, RawArray data) {
        WeakHashMap<String, WeakHashMap<Integer, SoftReference<RawArray>>> fileMap = null;
        while (fileMap == null) {
            fileMap = cache.get(backingFile);
            if (fileMap == null) {
                cache.putIfAbsent(backingFile, new WeakHashMap<String, WeakHashMap<Integer, SoftReference<RawArray>>>());
            }
        }
	WeakHashMap<Integer, SoftReference<RawArray>> branchMap = null;
	while (branchMap == null) {
            branchMap = fileMap.get(branch);
            if (branchMap == null) {
                fileMap.putIfAbsent(branch, new WeakHashMap<Integer, SoftReference<RawArray>>());
            }
        }
        branchMap.put(last, new SoftReference<RawArray>(data));
        return data;
    }

}
