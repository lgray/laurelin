package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.vanderbilt.accre.laurelin.Cache;
import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.root_proxy.Cursor;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFile;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFileCache;
import edu.vanderbilt.accre.laurelin.root_proxy.TBasket;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;

/**
 * Contains all the info needed to read a TBranch and its constituent TBaskets
 * without needing to deserialize the ROOT metadata -- i.e. this contains paths
 * and byte offsets to each basket
 */
public class SlimTBranch implements Serializable {
    private static final long serialVersionUID = 1L;
    private ROOTFile parentFile; // in the new schema the file is garaunteed open throughout the life cycle of SlimTBranch
    private long []basketEntryOffsets;
    private List<SlimTBasket> baskets;
    private TBranch.ArrayDescriptor arrayDesc;
    private int basketStartOffset;
    private int maxBasket;
    private String name;

    public SlimTBranch(ROOTFile parentFile, String name, long []basketEntryOffsets, TBranch.ArrayDescriptor desc, int basketStartOffset, int maxBasket) {
        this.parentFile = parentFile;
        this.basketEntryOffsets = basketEntryOffsets;
        this.baskets = new LinkedList<SlimTBasket>();
        this.arrayDesc = desc;
	this.basketStartOffset = basketStartOffset;
	this.maxBasket = maxBasket;
	this.name = name;
    }

    public static SlimTBranch getFromTBranch(TBranch fatBranch) {
        SlimTBranch slimBranch = new SlimTBranch(fatBranch.getTree().getBackingFile().getROOTFile(), 
						 fatBranch.getName(),
						 fatBranch.getBasketEntryOffsets(),
						 fatBranch.getArrayDescriptor(),
						 fatBranch.getBasketStart(),
						 fatBranch.getBasketStop());
        for (TBasket basket: fatBranch.getBaskets()) {
            SlimTBasket slimBasket = new SlimTBasket(slimBranch,
						     basket.getAbsoluteOffset(),
						     basket.getBasketBytes() - basket.getKeyLen(),
						     basket.getObjLen(),
						     basket.getKeyLen(),
						     basket.getLast()
						     );
            slimBranch.addBasket(slimBasket);
        }
        return slimBranch;
    }

    public String getName() {
	return name;
    }

    public long [] getBasketEntryOffsets() {
        return basketEntryOffsets;
    }

    public SlimTBasket getBasket(int basketid) {
	int relative_basket = basketid;
	if( this.basketStartOffset > -1 ) {
	    assert (this.maxBasket == -1 || basketid < this.maxBasket);
	    relative_basket = basketid - this.basketStartOffset;
	}
        return baskets.get(relative_basket);
    }

    public void addBasket(SlimTBasket basket) {
        baskets.add(basket);
    }

    public String getPath() {
        return parentFile.getPath();
    }

    public ROOTFile getParentFile() {
	return parentFile;
    }

    public TBranch.ArrayDescriptor getArrayDesc() {
        return arrayDesc;
    }

    /**
     * Glue callback to integrate with edu.vanderbilt.accre.laurelin.array
     * @param basketCache the cache we should be using
     * @param fileCache
     * @return GetBasket object used by array
     */
    public ArrayBuilder.GetBasket getArrayBranchCallback(Cache basketCache, ROOTFileCache fileCache) {
        return new BranchCallback(basketCache, this, fileCache);
    }

    /**
     * Glue callback to integrate with edu.vanderbilt.accre.laurelin.array
     * @param basketCache the cache we should be using
     * @return GetBasket object used by array
     */
    public ArrayBuilder.GetBasket getArrayBranchCallback(Cache basketCache) {
        return new BranchCallback(basketCache, this, null);
    }

    class BranchCallback implements ArrayBuilder.GetBasket {
        Cache basketCache;
        SlimTBranch branch;
        ROOTFileCache fileCache;

        public BranchCallback(Cache basketCache, SlimTBranch branch) {
            this(basketCache, branch, null);
        }

        public BranchCallback(Cache basketCache, SlimTBranch branch, ROOTFileCache fileCache) {
            this.basketCache = basketCache;
            this.branch = branch;
            this.fileCache = fileCache;
        }

        @Override
        public ArrayBuilder.BasketKey basketkey(int basketid) {
            SlimTBasket basket = branch.getBasket(basketid);
            return new ArrayBuilder.BasketKey(basket.getKeyLen(), basket.getLast(), basket.getObjLen());
        }

        @Override
        public RawArray dataWithoutKey(int basketid) {
            SlimTBasket basket = branch.getBasket(basketid);
            ROOTFile tmpFile;
            try {
                // the last event of each basket is guaranteed to be unique and
                // stable
                RawArray data = basketCache.get(parentFile, branch.getName(), basket.getLast());
                if (data == null) {
                    data = new RawArray(basket.getPayload());
                    basketCache.put(parentFile, branch.getName(), basket.getLast(), data);
                }
                return data;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class SlimTBasket implements Serializable {
        private static final Logger logger = LogManager.getLogger();

        private static final long serialVersionUID = 1L;
        private SlimTBranch branch;
        private long offset;
        private int compressedLen;
        private int uncompressedLen;
        private int keyLen;
        private int last;
        private Cursor payload;

        public SlimTBasket(SlimTBranch branch, long offset, int compressedLen, int uncompressedLen, int keyLen, int last) {
            this.branch = branch;
            this.offset = offset;
            this.compressedLen = compressedLen;
            this.uncompressedLen = uncompressedLen;
            this.keyLen = keyLen;
            this.last = last;
        }

        public int getKeyLen() {
            return keyLen;
        }

        public int getObjLen() {
            return uncompressedLen;
        }

        public int getLast() {
            return last;
        }
        private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
        public static String bytesToHex(byte[] bytes) {
            char[] hexChars = new char[bytes.length * 2];
            for ( int j = 0; j < bytes.length; j++ ) {
                int v = bytes[j] & 0xFF;
                hexChars[j * 2] = hexArray[v >>> 4];
                hexChars[j * 2 + 1] = hexArray[v & 0x0F];
            }
            return new String(hexChars);
        }
        private void initializePayload() throws IOException {
            ROOTFile tmpFile = branch.getParentFile(); 
            Cursor fileCursor = tmpFile.getCursor(offset);
            this.payload = fileCursor.getPossiblyCompressedSubcursor(0,
                    compressedLen,
                    uncompressedLen,
                    0);
        }

        public ByteBuffer getPayload(long offset, int len) throws IOException {
            if (this.payload == null) {
                initializePayload();
            }
            return this.payload.readBuffer(offset, len);
        }

        public ByteBuffer getPayload() throws IOException {
            if (this.payload == null) {
                initializePayload();
            }
            long len = payload.getLimit();
            return this.payload.readBuffer(0, uncompressedLen);
        }

    }

}
