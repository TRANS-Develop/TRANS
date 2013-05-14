package TRANS.Calculator;

import org.apache.hadoop.io.Writable;

public abstract class OptimusCalculator implements Writable{
	OptimusCalculator(){}
	abstract public Object calcOne(Object o1, Object o2);
	abstract public Object [] calcArray(Object []a1, Object []a2);
}
