package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

//

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    //Using items arraylist to store TDItems
    private ArrayList<TDItem> items = new ArrayList<TDItem>();

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        // use arraylist iterator
        return items.iterator(); //Testing
        
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here

        // reject if typeAr is < 1
        if (typeAr.length < 1) { //Not sure this part required
            throw new IllegalArgumentException("Must contain at least one entry");
        }


        for (int i = 0; i < typeAr.length; i++) {
            if (i > fieldAr.length) {
                // add null if fieldAr is too short
                items.add(new TDItem(typeAr[i], null));
            }
            else {
                items.add(new TDItem(typeAr[i], fieldAr[i]));
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        
        // reject if typeAr is < 1
        if (typeAr.length < 1) { //Not sure this part required
            throw new IllegalArgumentException("Must contain at least one entry");
        }

        // add names as null
        for (int i = 0; i < typeAr.length; i++) {
            items.add(new TDItem(typeAr[i], null));
        }

    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.size(); //Testing
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        // bad index
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException();
        }
        return items.get(i).fieldName; //Testing
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        // bad index
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException();
        }
        return items.get(i).fieldType; //Testing
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here

        // null is not a valid field name
        if (name == null) {
            throw new NoSuchElementException();
        }
        for( int i = 0; i < items.size(); i++) {
            if (items.get(i).fieldName != null && items.get(i).fieldName.equals(name)) {
                return i;
            }
        }

        throw new NoSuchElementException(); //Testing
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        // what the size in bytes??
        int size = 0;
        for (TDItem i: items){
            size += i.fieldType.getLen();

        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here

        // merged typeAr
        Type[] merge_typeAr = new Type[td1.numFields() + td2.numFields()];
        // merged fieldAr
        String[] merge_fieldAr = new String[td1.numFields() + td2.numFields()];

        // add td1 items
        for (int i = 0; i < td1.numFields(); i++) {
            merge_typeAr[i] = td1.getFieldType(i);
            merge_fieldAr[i] = td1.getFieldName(i);
        }

        // add td2 items
        for (int i = 0; i < td2.numFields(); i++) {
            merge_typeAr[i + td1.numFields()] = td2.getFieldType(i);
            merge_fieldAr[i + td1.numFields()] = td2.getFieldName(i);
        }

        return new TupleDesc(merge_typeAr, merge_fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == null) { //Object is null should return false
            return false;
        }

        if (o.getClass() != this.getClass()) { //Object is not a TupleDesc should return false
            return false;
        }

        TupleDesc object = (TupleDesc) o;


        if (items.size() != object.numFields()) { // not the same size should return false
            return false;
        }

        for (int i = 0; i < items.size(); i++) {
            // if the type is not the same should return false
            if ((items.get(i).fieldType != object.getFieldType(i))){
                return false;
            }
        }

        return true;

    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return super.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String result = "";
        for (TDItem item : items) {
            result += item.toString() + ", ";
        }
        result = result.substring(0, result.length() - 2);
        return result;
    }
}
