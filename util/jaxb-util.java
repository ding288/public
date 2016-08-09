package org.di.util;

import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

/**
 * @author d
 *
 */
public class JaxbUtil {
	public static Object xml2pojo(String xml,Class<?> targetClass) {
		Object o = null;
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(targetClass);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			o = jaxbUnmarshaller.unmarshal(new StringReader(xml));
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return o;
	}

	public static String pojo2xml(Object o) {
		StringWriter sw = new StringWriter();
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(o.getClass());
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.marshal(o, sw);
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return sw.toString();
	}

	public static void main(String[] args) {
	}
}
