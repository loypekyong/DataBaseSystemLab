package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> groupAggregateValue;
    private Map<Field, Integer> groupCount;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT && what != Op.SUM && what != Op.AVG && what != Op.MIN && what != Op.MAX) {
            throw new IllegalArgumentException("Only COUNT, SUM, AVG, MIN, and MAX are supported.");
        }

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupAggregateValue = new HashMap<>();
        this.groupCount = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupVal = (gbfield == Aggregator.NO_GROUPING) ? null : tup.getField(gbfield);
        int aggregateVal = ((IntField) tup.getField(afield)).getValue();
        groupAggregateValue.putIfAbsent(groupVal, (what == Op.MIN) ? Integer.MAX_VALUE : 0);
        groupCount.putIfAbsent(groupVal, 0);
        int currentAggregate = groupAggregateValue.get(groupVal);
        int currentCount = groupCount.get(groupVal);

        switch (what) {
            case COUNT:
                currentAggregate++;
                break;
            case SUM:
                currentAggregate += aggregateVal;
                break;
            case AVG: // keeping the SUM and COUNT, calculate AVG in iterator()
            case MIN:
                currentAggregate = Math.min(currentAggregate, aggregateVal);
                break;
            case MAX:
                currentAggregate = Math.max(currentAggregate, aggregateVal);
                break;
        }

        groupAggregateValue.put(groupVal, currentAggregate);
        groupCount.put(groupVal, currentCount + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> results = new ArrayList<>();
        TupleDesc td = (gbfield == Aggregator.NO_GROUPING) ?
                new TupleDesc(new Type[]{Type.INT_TYPE}) :
                new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});

        for (Map.Entry<Field, Integer> entry : groupAggregateValue.entrySet()) {
            int value = entry.getValue();
            if (what == Op.AVG) {
                value /= groupCount.get(entry.getKey());
            }

            Tuple tuple = new Tuple(td);
            if (gbfield == Aggregator.NO_GROUPING) {
                tuple.setField(0, new IntField(value));
            } else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(value));
            }
            results.add(tuple);
        }

        return new TupleIterator(td, results);
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
    }

}
