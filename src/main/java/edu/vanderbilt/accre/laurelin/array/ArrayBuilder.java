package edu.vanderbilt.accre.laurelin.array;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public class ArrayBuilder {

    /**
     * Subset of only the values in TKey which describes the basket on-disk.
     *
     */
    public static class BasketKey {
        int fKeylen;
        int fLast;
        int fObjlen;

        public BasketKey(int fKeylen, int fLast, int fObjlen) {
            this.fKeylen = fKeylen;
            this.fLast = fLast;
            this.fObjlen = fObjlen;
        }
    }

    /**
     * Callback interface used by the array interface to request additional info
     * about baskets from the root_proxy layer
     */
    public static interface GetBasket {
        /**
         * Get the BasketKey describing a certain basketid.
         * @param basketid the zero-indexed basket index for the given branch
         * @return BasketKey filled with info about the chosen basket
         */
        public BasketKey basketkey(int basketid);

        /**
         * Retrieves the decompressed bytes within the basket, excluding the
         * TKey header.
         * @param basketid the zero-indexed basket index for the given branch
         * @return a RawArray with the decompressed bytes
         */
        public RawArray dataWithoutKey(int basketid);   // length must be fObjlen - fKeylen
    }

    /**
     * Callbacks to get info about a basket from root_proxy.
     */
    Interpretation interpretation;
    int[] basket_itemoffset;
    int[] basket_entryoffset;
    Array array;
    ArrayList<FutureTask<Boolean>> tasks;

    public ArrayBuilder(GetBasket getbasket, Interpretation interpretation, long[] basketEntryOffsets, Executor executor, long entrystart, long entrystop) {
        this.interpretation = interpretation;

        if (basketEntryOffsets.length == 0  ||  basketEntryOffsets[0] != 0) {
            throw new IllegalArgumentException("basketEntryOffsets must start with zero");
        }
        for (int i = 1;  i < basketEntryOffsets.length;  i++) {
            if (basketEntryOffsets[i] < basketEntryOffsets[i - 1]) {
                throw new IllegalArgumentException("basketEntryOffsets must be monotonically increasing " + 
						   Integer.toString(i) + " / " + Integer.toString(basketEntryOffsets.length) +
						   ": "  + Long.toString(basketEntryOffsets[i]) +
						   " ?>? " + Long.toString(basketEntryOffsets[i - 1]) +
						   " offsets: " + Arrays.toString(basketEntryOffsets));
            }
        }

        int basketstart = -1;
        int basketstop = -1;

        for (int i = 0;  i < basketEntryOffsets.length - 1;  i++) {
            if (basketstart == -1) {
                if (entrystart < basketEntryOffsets[i + 1]  &&  basketEntryOffsets[i] < entrystop) {
                    basketstart = i;
                    basketstop = i;
                }
            } else {
                if (basketEntryOffsets[i] < entrystop) {
                    basketstop = i;
                }
            }
        }

        if (basketstop != -1) {
            basketstop += 1;
        }

        if (basketstart == -1) {
            basket_itemoffset = null;
            basket_entryoffset = null;
            array = interpretation.empty();
        } else {
            BasketKey[] basketkeys = new BasketKey[basketstop - basketstart];
            for (int j = 0;  j < basketstop - basketstart;  j++) {
                basketkeys[j] = getbasket.basketkey(basketstart + j);
            }

            long totalitems = 0;
            long totalentries = 0;
            basket_itemoffset = new int[1 + basketstop - basketstart];
            basket_entryoffset = new int[1 + basketstop - basketstart];

            basket_itemoffset[0] = 0;
            basket_entryoffset[0] = 0;
            for (int j = 1;  j < 1 + basketstop - basketstart;  j++) {
                long numentries = basketEntryOffsets[j + basketstart] - basketEntryOffsets[j + basketstart - 1];
                totalentries += numentries;
                if (totalentries != (int)totalentries) {
                    throw new IllegalArgumentException("number of entries requested of ArrayBuilder.build must fit into a 32-bit integer");
                }

                int numbytes = basketkeys[j - 1].fLast - basketkeys[j - 1].fKeylen;
                long numitems = interpretation.numitems(numbytes, (int)numentries);

                totalitems += numitems;
                if (totalitems != (int)totalitems) {
                    throw new IllegalArgumentException("number of items requested of ArrayBuilder.build must fit into a 32-bit integer");
                }

                basket_itemoffset[j] = basket_itemoffset[j - 1] + (int)numitems;
                basket_entryoffset[j] = basket_entryoffset[j - 1] + (int)numentries;
            }

            array = interpretation.destination((int)totalitems, (int)totalentries);

            tasks = new ArrayList<FutureTask<Boolean>>(basketstop - basketstart);

	    int partitionItemOffset = 0;
	    int partitionEntryOffset = 0;

	    if (basketstart > 0) {
		// if we're later on in the root file we need to offset the callable fill in a global way
		int local_entrystart = (int)(entrystart - basketEntryOffsets[basketstart]);
		if (local_entrystart < 0) {
		    local_entrystart = 0;
		}
		int local_entrystop = (int)(entrystop - basketEntryOffsets[basketstart]);
		if (local_entrystop > (int)(basketEntryOffsets[basketstart + 1] - basketEntryOffsets[basketstart])) {
		    local_entrystop = (int)(basketEntryOffsets[basketstart + 1] - basketEntryOffsets[basketstart]);
		}
		if (local_entrystop < 0) {
		    local_entrystop = 0;
		}
		partitionEntryOffset  = local_entrystart;

		// now to get the item offset
		RawArray basketdata = getbasket.dataWithoutKey(basketstart);

		Array source = null;
		int border = basketkeys[0].fLast - basketkeys[0].fKeylen;

		if (basketkeys[0].fObjlen == border) {
		    basketdata = interpretation.convertBufferDiskToMemory(basketdata);
		    source = interpretation.fromroot(basketdata, null, partitionEntryOffset, local_entrystop);
		} else {
		    RawArray content = basketdata.slice(0, border);
		    PrimitiveArray.Int4 byteoffsets = new PrimitiveArray.Int4(basketdata.slice(border + 4, basketkeys[0].fObjlen)).add(true, -basketkeys[0].fKeylen);
		    byteoffsets.put(byteoffsets.length() - 1, border);
		    content = interpretation.subarray().convertBufferDiskToMemory(content);
		    byteoffsets = interpretation.subarray().convertOffsetDiskToMemory(byteoffsets);
		    source = interpretation.fromroot(content, byteoffsets, local_entrystart, local_entrystop);
		}

		int expecteditems = basket_itemoffset[1] - basket_itemoffset[0];
		int source_numitems = interpretation.source_numitems(source);
		partitionItemOffset = expecteditems - source_numitems;
	    }

	    for(int j = 0; j < basketstop - basketstart; j++) {
		basket_itemoffset[j] -= partitionItemOffset;
		basket_entryoffset[j] -= partitionEntryOffset;
	    }

            for (int j = 0;  j < basketstop - basketstart;  j++) {
                CallableFill fill = new CallableFill(interpretation, getbasket, j, basketkeys, array, entrystart, entrystop, basketstart, basketstop, basket_itemoffset, basket_entryoffset, basketEntryOffsets,
						     partitionItemOffset, partitionEntryOffset);

                if (executor == null) {
                    fill.call();
                }
                else {
                    FutureTask<Boolean> task = new FutureTask<Boolean>(fill);
                    tasks.add(task);
                    executor.execute(task);
                }
            }
        }
    }

    public Array getArray(int rowId, int count) {
        for (FutureTask<Boolean> task : tasks) {
            try {
                task.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e.toString());
            } catch (ExecutionException e) {
                throw new RuntimeException(e.toString());
            }
        }
        return array.clip(basket_entryoffset[0] + rowId, basket_entryoffset[0] + rowId + count);
    }

    private static class CallableFill implements Callable<Boolean> {
        Interpretation interpretation;
        GetBasket getbasket;
        int j;
        BasketKey[] basketkeys;
        Array destination;
        long entrystart;
        long entrystop;
        int basketstart;
        int basketstop;
        int[] basket_itemoffset;
        int[] basket_entryoffset;
        long[] basketEntryOffsets;
	int partitionEntryOffset;
	int partitionItemOffset;

        CallableFill(Interpretation interpretation, GetBasket getbasket, int j, BasketKey[] basketkeys, Array destination, long entrystart, long entrystop, int basketstart, int basketstop, int[] basket_itemoffset, int[] basket_entryoffset, long[] basketEntryOffsets, int partitionItemOffset, int partitionEntryOffset) {
            this.interpretation = interpretation;
            this.getbasket = getbasket;
            this.j = j;
            this.basketkeys = basketkeys;
            this.destination = destination;
            this.entrystart = entrystart;
            this.entrystop = entrystop;
            this.basketstart = basketstart;
            this.basketstop = basketstop;
            this.basket_itemoffset = basket_itemoffset;
            this.basket_entryoffset = basket_entryoffset;
            this.basketEntryOffsets = basketEntryOffsets;
	    this.partitionItemOffset = partitionItemOffset;
	    this.partitionEntryOffset = partitionEntryOffset;
        }

        @Override
        public Boolean call() {
            // https://github.com/scikit-hep/uproot/blob/3.8.0/uproot/tree.py#L1361
            // https://github.com/scikit-hep/uproot/blob/3.8.0/uproot/tree.py#L1132

            int i = j + basketstart;

            int local_entrystart = (int)(entrystart - basketEntryOffsets[i]);
            if (local_entrystart < 0) {
                local_entrystart = 0;
            }
            int local_entrystop = (int)(entrystop - basketEntryOffsets[i]);
            if (local_entrystop > (int)(basketEntryOffsets[i + 1] - basketEntryOffsets[i])) {
                local_entrystop = (int)(basketEntryOffsets[i + 1] - basketEntryOffsets[i]);
            }
            if (local_entrystop < 0) {
                local_entrystop = 0;
            }
	    
            RawArray basketdata = getbasket.dataWithoutKey(i);

            Array source = null;
            int border = basketkeys[j].fLast - basketkeys[j].fKeylen;

            if (basketkeys[j].fObjlen == border) {
                basketdata = interpretation.convertBufferDiskToMemory(basketdata);
                source = interpretation.fromroot(basketdata, null, local_entrystart, local_entrystop);
            } else {
                RawArray content = basketdata.slice(0, border);
                PrimitiveArray.Int4 byteoffsets = new PrimitiveArray.Int4(basketdata.slice(border + 4, basketkeys[j].fObjlen)).add(true, -basketkeys[j].fKeylen);
                byteoffsets.put(byteoffsets.length() - 1, border);
                content = interpretation.subarray().convertBufferDiskToMemory(content);
                byteoffsets = interpretation.subarray().convertOffsetDiskToMemory(byteoffsets);
                source = interpretation.fromroot(content, byteoffsets, local_entrystart, local_entrystop);
            }

            int expecteditems = basket_itemoffset[j + 1] - basket_itemoffset[j];
            int source_numitems = interpretation.source_numitems(source);

            int expectedentries = basket_entryoffset[j + 1] - basket_entryoffset[j];
            int source_numentries = local_entrystop - local_entrystart;

            if ( (j + 1 == basketstop - basketstart) && (basketstop - basketstart > 2) ) {
                if (expecteditems > source_numitems) {
                    basket_itemoffset[j + 1] -= expecteditems - source_numitems;
                }
                if (expectedentries > source_numentries) {
                    basket_entryoffset[j + 1] -= expectedentries - source_numentries;
                }
            } else if (j == 0) {
		if (expecteditems > source_numitems) {
		    basket_itemoffset[j] += expecteditems - source_numitems;
		}
		if (expectedentries > source_numentries) {
		    basket_entryoffset[j] += expectedentries - source_numentries;
		}
            }
	    
            interpretation.fill(source,
                                destination,
                                basket_itemoffset[j],
                                basket_itemoffset[j + 1],
                                basket_entryoffset[j],
                                basket_entryoffset[j + 1]);

            return true;
        }
    }
}
