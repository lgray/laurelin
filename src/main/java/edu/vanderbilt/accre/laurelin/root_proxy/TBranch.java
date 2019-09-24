package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TBranch {
    protected Proxy data;
    protected ArrayList<TBranch> branches;
    private ArrayList<TLeaf> leaves;
    private ArrayList<TBasket> baskets;

    protected boolean isBranch;
    protected TBranch parent;
    protected TTree tree;

    // metadata caches
    private int fMaxBaskets;
    private int fActualBaskets;
    private int[] fBasketBytes;
    private long[] fBasketEntry;
    private long[] fBasketSeek;

    //short circuit
    private long entryStart;
    private long entryStop;
    private int basketStart;
    private int basketStop;


    public static class ArrayDescriptor implements Serializable {
        private static final long serialVersionUID = 1L;
        private boolean isFixed;
        private int fixedLength;
        private String branchName;
        private int skipBytes;

        public static ArrayDescriptor newNumArray(String mag, int skipBytes) {
            ArrayDescriptor ret = new ArrayDescriptor();
            ret.isFixed = true;
            ret.fixedLength = Integer.parseInt(mag);
            ret.skipBytes = skipBytes;
            return ret;
        }

        public static ArrayDescriptor newNumArray(String mag) {
            return newNumArray(mag, 0);
        }

        public static ArrayDescriptor newVarArray(String mag, int skipBytes) {
            ArrayDescriptor ret = new ArrayDescriptor();
            ret.isFixed = false;
            ret.branchName = mag;
            ret.skipBytes = skipBytes;
            return ret;
        }

        public static ArrayDescriptor newVarArray(String mag) {
            return newVarArray(mag, 0);
        }

        public boolean isFixed() {
            return isFixed;
        }

        public int getFixedLength() {
            return fixedLength;
        }

        public int getSkipBytes() {
            return skipBytes;
        }
    }

    public TBranch(Proxy data, TTree tree, TBranch parent, long entryStart, long entryStop) {
        this.data = data;
        this.parent = parent;
        this.tree = tree;
	this.entryStart = entryStart;
	this.entryStop = entryStop;
	this.basketStart = -1;
	this.basketStop = -1;

	this.fMaxBaskets = -1;
	this.fActualBaskets = -1;
        this.fBasketBytes = null;
        this.fBasketEntry = null;
        this.fBasketSeek = null;

        branches = new ArrayList<TBranch>();
        if (getClass().equals(TBranch.class)) {
            baskets = null; // baskets will be evaluated lazily
            leaves = new ArrayList<TLeaf>();
            isBranch = true;
            ProxyArray fBranches = (ProxyArray) data.getProxy("fBranches");
            for (Proxy val: fBranches) {
                TBranch branch = new TBranch(val, tree, this, entryStart, entryStop);
                // Drop branches with neither subbranches nor leaves
                if (branch.getBranches().size() != 0 || branch.getLeaves().size() != 0) {
                    if (branch.getName().startsWith("P3")) {
                        continue;
                    }
                    branches.add(branch);
                }
            }
            ProxyArray fLeaves = (ProxyArray) data.getProxy("fLeaves");
            for (Proxy val: fLeaves) {
                TLeaf leaf = new TLeaf(val, tree, this, this.entryStart, this.entryStop);
                if (leaf.typeUnhandled()) {
                    continue;
                }
                leaves.add(leaf);
            }
        } else {
            isBranch = false;
        }
    }

    public TTree getTree() {
        return tree;
    }

    public String getTitle() {
        return (String) data.getScalar("fTitle").getVal();
    }

    public String getName() {
        return (String) data.getScalar("fName").getVal();
    }

    public String getFullName() {
        String ret;
        ret = (String) data.getScalar("fName").getVal();
        if (ret.endsWith(".")) {
            ret = ret.substring(0, ret.length() - 1);
        }
        return ret;
    }

    public String getClassName() {
        return data.getClassName();
    }

    public ArrayList<TBranch> getBranches() {
        return branches;
    }

    public List<TLeaf> getLeaves() {
        return leaves;
    }

    public int getBasketStart() {
	return this.basketStart;
    }

    public int getBasketStop() {
	return this.basketStop;
    }

    /**
     * returns fType
     * @return ROOT type integer constant
     */
    public Integer getType() {
        return (Integer) data.getScalar("fType").getVal();
    }

    /**
     * returns normalized fType
     * @return ROOT type integer constant
     */
    public Integer getNormedType() {
        Integer fType = (Integer) data.getScalar("fType").getVal();
        if ((Constants.kOffsetL < fType) && (fType < Constants.kOffsetP)) {
            fType = fType - Constants.kOffsetL;
        }
        return fType;
    }

    private void lazyGetBasketDescription() {
	this.fMaxBaskets = (int) this.data.getScalar("fMaxBaskets").getVal();
        this.fBasketBytes = (int[]) this.data.getScalar("fBasketBytes").getVal();
        this.fBasketEntry = (long[]) this.data.getScalar("fBasketEntry").getVal();
        this.fBasketSeek = (long[]) this.data.getScalar("fBasketSeek").getVal();

	long[] offsets = new long[this.fMaxBaskets + 1];
	this.fActualBaskets = 0;
        for (int i = 0; i < this.fMaxBaskets; ++i) {
	    if( this.fBasketSeek[i] != 0 ) {
		offsets[this.fActualBaskets] = this.fBasketEntry[i];
		++(this.fActualBaskets);
	    }
        }
        offsets[this.fActualBaskets] = this.tree.getEntries();

	for (int i = 1;  i < this.fActualBaskets;  i++) {
            if (this.fBasketEntry[i] < this.fBasketEntry[i - 1]) {
                throw new IllegalArgumentException("fBasketEntry must be monotonically increasing " +
                                                   Integer.toString(i) + " / " + Integer.toString(this.fBasketEntry.length) +
                                                   ": "  + Long.toString(this.fBasketEntry[i]) +
                                                   " ?>? " + Long.toString(this.fBasketEntry[i - 1]) +
                                                   " offsets: " + Arrays.toString(this.fBasketEntry) + 
						   " bytes: " + Arrays.toString(this.fBasketBytes) +
						   " seek: " + Arrays.toString(this.fBasketSeek) +
						   " file: " + this.tree.getBackingFile().getFileName());
            }
        }

	for (int i = 1;  i <= this.fActualBaskets;  i++) {
            if (offsets[i] < offsets[i - 1]) {
		throw new IllegalArgumentException("offsets must be monotonically increasing " +
                                                   Integer.toString(i) + " / " + Integer.toString(offsets.length) +
                                                   ": "  + Long.toString(offsets[i]) +
                                                   " ?>? " + Long.toString(offsets[i - 1]) +
                                                   " offsets: " + Arrays.toString(offsets));
            }
        }

	for (int i = 0; i <= this.fActualBaskets; ++i) {
            if( this.entryStart != -1 && offsets[i+1] < this.entryStart) continue;
            else if ( this.entryStart != -1 && this.basketStart == -1 ) this.basketStart = i;
            if( this.entryStop != -1 && this.entryStop <= offsets[i]) {
                if( this.basketStop == -1 ) this.basketStop = i+1;
                continue;
            }
	}
	if( this.entryStart == -1 ) this.basketStart = 0;
        if( this.entryStop == -1 ) this.basketStop = this.fActualBaskets;
	
    }
    

    private ArrayList<TBasket> lazyGetBaskets() {
	/*
	 * Instead of being in the ObjArray of fBaskets, ROOT stores the baskets in separate
	 * toplevel entries in the file
	 *     Int_t      *fBasketBytes;      ///<[fMaxBaskets] Length of baskets on file
	 *     Long64_t   *fBasketEntry;      ///<[fMaxBaskets] Table of first entry in each basket
	 *     Long64_t   *fBasketSeek;       ///<[fMaxBaskets] Addresses of baskets on file
	 */
	if( this.fMaxBaskets == -1 ) { lazyGetBasketDescription(); }
	ArrayList<TBasket> thebaskets = new ArrayList<TBasket>();

	long[] offsets = new long[this.fMaxBaskets + 1];
        for (int i = 0; i < this.fMaxBaskets; ++i) {
	    offsets[i] = this.fBasketEntry[i];
        }
	offsets[this.fMaxBaskets] = this.tree.getEntries();
	
	TFile backing = this.tree.getBackingFile();
	for (int i = this.basketStart; i < this.basketStop; ++i) {
	    Cursor c;
	    if (fBasketSeek[i] == 0) {
		// An empty basket?
		continue;
	    }
	    try {
		c = backing.getCursorAt(fBasketSeek[i]);
		TBasket b = TBasket.getFromFile(c, fBasketBytes[i], fBasketEntry[i], fBasketSeek[i]);
		System.out.print(this.getName() + " got basket with last event -> " + Integer.toString(b.getLast()) + "\n");
		thebaskets.add(b);
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	assert this.fActualBaskets == thebaskets.size();

	return thebaskets;
    }

    public List<TBasket> getBaskets() {
	if( this.baskets == null ) {
	    this.baskets = lazyGetBaskets();
	}	
        return this.baskets;
    }

    /*
     * Note: this will all have to be refactored to support multidimensional
     *       arrays, but sufficient is today for its own troubles
     */

    // really wish java had a non-escaped string specifier
    // I wanna grab everything out of the square brackets
    //                                          \[(\d+)\]
    Pattern arrayNumPattern = Pattern.compile("\\[(\\d+)\\]");
    //                                          \[([^\]]+)\]
    Pattern arrayVarPattern = Pattern.compile("\\[([^\\]]+)\\]");

    /**
     * Returns an ArrayDescriptor describing this branch
     *
     * @return ArrayDescriptor containing the array params if this is an array
     *         null otherwise
     */
    public ArrayDescriptor getArrayDescriptor() {
        if (getLeaves().size() == 0) {
            return null;
        } else  if (getLeaves().size() != 1) {
            throw new RuntimeException("Non-split branches are not supported");
        }
        ArrayDescriptor ret = null;
        TLeaf leaf = getLeaves().get(0);
        String title = leaf.getTitle();

        Object className = this.data.getScalar("fClassName");
        if ((ret == null) && (className != null)) {
            switch ((String)this.data.getScalar("fClassName").getVal()) {
                case "vector<bool>":
                case "vector<char>":
                case "vector<unsigned char>":
                case "vector<short>":
                case "vector<unsigned short>":
                case "vector<int>":
                case "vector<unsigned int>":
                case "vector<long>":
                case "vector<unsigned long>":
                case "vector<float>":
                case "vector<double>":
                    /*
                     *  need to treat this as a float array and skip the
                     *  first 10 bytes to skip the vector stuff. See Uproot
                     *  interp/auto.py
                     */
                    return ArrayDescriptor.newVarArray("", 10);
                default:
                    break;
            }
        }

        if (!title.contains("[")) {
            // no square brackets means no possibility of being an array
            return null;
        } else if (title.indexOf("[") != title.lastIndexOf(("["))) {
            throw new RuntimeException("Multidimensional arrays are not supported");
        } else {
            Matcher numMatcher = arrayNumPattern.matcher(title);
            Matcher varMatcher = arrayVarPattern.matcher(title);
            if (numMatcher.find()) {
                return ArrayDescriptor.newNumArray(numMatcher.group(1));
            } else if (varMatcher.find()) {
                return ArrayDescriptor.newVarArray(varMatcher.group(1));
            } else {
                throw new RuntimeException("Unable to parse array indices");
            }
        }
    }

    public boolean typeUnhandled() { return false; }

    public SimpleType getSimpleType() {
        SimpleType ret = null;
        if (leaves.size() == 1) {
            TLeaf leaf = leaves.get(0);
            if (getTitle().length() >= 2) {
                ret = getTypeFromTitle(getTitle());
            }

            Object className = this.data.getScalar("fClassName");
            if ((ret == null) && (className != null)) {
                switch ((String)this.data.getScalar("fClassName").getVal()) {
                    // See Uproot interp/auto.py
                    case "vector<bool>":
                        ret = new SimpleType.ArrayType(SimpleType.Bool);
                        break;
                    case "vector<char>":
                        ret = new SimpleType.ArrayType(SimpleType.Int8);
                        break;
                    case "vector<unsigned char>":
                        ret = new SimpleType.ArrayType(SimpleType.UInt8);
                        break;
                    case "vector<short>":
                        ret = new SimpleType.ArrayType(SimpleType.Int16);
                        break;
                    case "vector<unsigned short>":
                        ret = new SimpleType.ArrayType(SimpleType.UInt16);
                        break;
                    case "vector<int>":
                        ret = new SimpleType.ArrayType(SimpleType.Int32);
                        break;
                    case "vector<unsigned int>":
                        ret = new SimpleType.ArrayType(SimpleType.UInt32);
                        break;
                    case "vector<long>":
                        ret = new SimpleType.ArrayType(SimpleType.Int64);
                        break;
                    case "vector<unsigned long>":
                        ret = new SimpleType.ArrayType(SimpleType.UInt64);
                        break;
                    case "vector<float>":
                        ret = new SimpleType.ArrayType(SimpleType.Float32);
                        break;
                    case "vector<double>":
                        ret = new SimpleType.ArrayType(SimpleType.Float64);
                        break;
                    default:
                        break;
                }
            }

            if (ret == null) {
                ret = leaf.getLeafType();
            }
        }
        if (ret == null) {
            throw new RuntimeException("Unknown simple type for branch named: " + this.getName());
        }

        return ret;
    }

    protected SimpleType getTypeFromTitle(String title) {
        SimpleType ret = null;
        String lastTwo = title.substring(title.length() - 2, title.length());
        if (lastTwo.charAt(0) == '/') {
            switch (lastTwo) {
                case ("/B"):
                    ret = SimpleType.Int8;
                    break;
                case ("/b"):
                    ret = SimpleType.UInt8;
                    break;
                case ("/S"):
                    ret = SimpleType.Int16;
                    break;
                case ("/s"):
                    ret = SimpleType.UInt16;
                    break;
                case ("/I"):
                    ret = SimpleType.Int32;
                    break;
                case ("/i"):
                    ret = SimpleType.UInt32;
                    break;
                case ("/L"):
                    ret = SimpleType.Int64;
                    break;
                case ("/l"):
                    ret = SimpleType.UInt64;
                    break;
                case ("/O"):
                    ret = SimpleType.Bool;
                    break;
                case ("/F"):
                    ret = SimpleType.Float32;
                    break;
                case ("/D"):
                    ret = SimpleType.Float64;
                    break;
                default:
                    throw new RuntimeException("Unknown branch type: " + lastTwo + " name is: " + title);
            }
            // Do I later want to separate fixed and not-fixed arrays?
            if (title.contains("[")) {
                ret = new SimpleType.ArrayType(ret);
            }
        }
        return ret;
    }

    public long[] getBasketEntryOffsets() {
	if( this.fActualBaskets == -1 ) { lazyGetBasketDescription(); }
        // The array processing code wants a final entry to cap the last true
        // basket from above
	long []ret = new long[this.fActualBaskets + 1];
        for (int i = 0; i < this.fActualBaskets; i += 1) {
            ret[i] = this.fBasketEntry[i];
        }
        ret[this.fActualBaskets] = tree.getEntries();
        return ret;
    }

}
