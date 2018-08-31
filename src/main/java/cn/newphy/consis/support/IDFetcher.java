package cn.newphy.consis.support;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.FieldCallback;
import org.springframework.util.ReflectionUtils.FieldFilter;

import cn.newphy.consis.BizId;

public class IDFetcher {

	/**
	 * 获得编号
	 * @param obj
	 * @return
	 */
	public static String getId(final Object obj) {
		if(obj == null) {
			return null;
		}
		final Set<Object> set = new HashSet<Object>();
		Class<?> clazz = obj.getClass();
		ReflectionUtils.doWithFields(clazz, new FieldCallback() {
			@Override
			public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
				try {
					PropertyDescriptor propertyDescriptor = BeanUtils.getPropertyDescriptor(obj.getClass(), field.getName());
					if(propertyDescriptor != null && propertyDescriptor.getReadMethod() != null) {
						Object value = getPropertyValue(obj, field.getName());
						set.add(value == null ? "" : value.toString());		
					}
				} catch (Exception e) {
					set.add("");
				}
			}
		}, new FieldFilter(){
			@Override
			public boolean matches(Field field) {
				return field.isAnnotationPresent(BizId.class);
			}}
		);
		
		Object value = set.isEmpty() ? null : set.iterator().next();
		if(value == null) {
			PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(clazz);
			for (int i = 0; i < pds.length; i++) {
				if(pds[i].getReadMethod() != null && pds[i].getReadMethod().isAnnotationPresent(BizId.class)) {
					try {
						
						value = getPropertyValue(obj, pds[i].getName());
					} catch (Exception e) {
						value = "";
					}
					break;
				}
			}
		}
		
		if(value == null) {
			try {
				value = getPropertyValue(obj, "id");
			} catch (Exception e) {
				value = null;
			}
		}
		return value == null ? "" : value.toString();
	}
	
	private static Object getPropertyValue(Object obj, String propertyName) {
		BeanWrapper beanWrapper = new BeanWrapperImpl(obj.getClass());
		return beanWrapper.getPropertyValue(propertyName);
	}
}
