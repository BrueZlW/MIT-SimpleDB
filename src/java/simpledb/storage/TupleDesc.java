package simpledb.storage;


import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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
        // some code done

        return new Iterator<TDItem>() {
            //指针
            int ptr = -1;
            @Override
            public boolean hasNext() {
                ptr++;
                return ptr < types.length - 1;
            }

            @Override
            public TDItem next() {
                if (hasNext()){
                    ptr++;
                    return new TDItem(types[ptr], fields[ptr]);
                }
                return null;
            }
        };
    }

    private static final long serialVersionUID = 1L;
    private Type[] types;
    private String[] fields;

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
        // some code done
        this.types = typeAr;
        this.fields = fieldAr;
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
        if (typeAr.length >= 1){
            this.types = typeAr;
        }

        // some code done
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code done
        if (types == null)return 0;

        return types.length;
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
        if(i < 0 || i >= numFields()) throw  new NoSuchElementException("数组越界");
        return fields[i];
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
        if(i < 0 || i >= numFields()) throw  new NoSuchElementException("数组越界");
        return types[i];
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
        if(this.fields == null) throw new NoSuchElementException("没有fields");

        for(int i = 0; i < fields.length; i++) {
            if (fields[i].equals(name)){
                return i;
            }
        }

        throw new NoSuchElementException("no Such Field");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (Type type : types) {
            size += type.getLen();
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
        int len1 = td1.types.length;
        int len2 = td2.types.length;

        Type[] totalTypes = new Type[len1 + len2];
        String[] totalFields = new String[len1 + len2];
        for (int i = 0; i < len1 + len2; i++) {
            if (i < len1){
                totalFields[i] = td1.fields[i];
                totalTypes[i] = td1.types[i];
            }else {
                totalFields[i] = td2.fields[i - len1];
                totalTypes[i] = td2.types[i - len1];
            }
        }
        return new TupleDesc(totalTypes, totalFields);
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
        if( !(o instanceof TupleDesc)) return false;

        if(this == null || this.types == null) return false;

        if(this.numFields() != ((TupleDesc) o).numFields()) return false;

        for(int i = 0; i < numFields(); i++) {
            if(this.types[i] != ((TupleDesc) o).types[i]) return  false;
        }

        if(this.fields != null) {

            if(((TupleDesc) o).fields == null) return false;

            for(int i = 0; i < numFields(); i++) {
                if(this.fields[i] != ((TupleDesc) o).fields[i]) return  false;
            }

        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results

//        throw new UnsupportedOperationException("unimplemented");

        return Objects.hash(this);
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
        StringBuilder sb = new StringBuilder();
        while (iterator().hasNext()) {
            TDItem tdItem = iterator().next();
            if (tdItem.fieldName != null) {
                sb.append(tdItem.fieldType + "(" + tdItem.fieldName + ", ");
            }
        }
        return sb.length() == 0 ? "" : sb.substring(0, sb.length() - 3);
    }
}
