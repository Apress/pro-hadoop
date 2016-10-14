package com.apress.hadoopbook;

import java.lang.reflect.Field;

/** Use reflection to get at private member fields of objects.
 */
public class FieldMappingData<T,F> {
	final String className;
	final String fieldName;
	final Class<? extends T> working;
	final Field field;
	final Throwable failed;
	@SuppressWarnings("unchecked")
	public FieldMappingData( final String className, final String fieldName) {
		this.className = className;
		this.fieldName = fieldName;
		Class<? extends T> workingDummy = null;
		Field fieldDummy = null;
		Throwable failedDummy = null;
		try {
			Class<?> dummy1 = Class.forName(className);
			workingDummy = (Class<? extends T>) dummy1;
		
			Field[] fields = workingDummy.getDeclaredFields();
            for( Field field : fields ) {
                if (this.fieldName.equals(field.getName())
                		) {
                	fieldDummy = field;
                    fieldDummy.setAccessible(true);
                    break;
                }
            }
		} catch( Throwable e) {
			failedDummy = e;
		} finally {
			failed = failedDummy;
			working = (Class<? extends T>) workingDummy;
			field = fieldDummy;
		}
	}
	@SuppressWarnings("unchecked")
	public
	F getField( T obj) throws IllegalAccessException {
		if (failed!=null) {
			return null;
		}
		return (F) field.get(obj);
	}
	public boolean isValid() {
		return failed==null;
	}
	/**
	 * @return the className
	 */
	public String getClassName() {
		return className;
	}
	/**
	 * @return the fieldName
	 */
	public String getFieldName() {
		return fieldName;
	}
	/**
	 * @return the working
	 */
	public Class<? extends T> getWorking() {
		return working;
	}
	/**
	 * @return the field
	 */
	public Field getField() {
		return field;
	}
	/**
	 * @return the failed
	 */
	public Throwable getFailed() {
		return failed;
	}

}