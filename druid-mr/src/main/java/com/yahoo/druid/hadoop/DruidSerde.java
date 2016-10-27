package com.yahoo.druid.hadoop;

import io.druid.data.input.MapBasedInputRow;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DruidSerde implements SerDe{
	private static final Logger logger = LoggerFactory.getLogger(DruidSerde.class);

	private List<String> columnNames;
	List<TypeInfo> columnTypes;
	StructTypeInfo rowTypeInfo;
	private ObjectInspector objectInspector;
	private List<Object> row = new ArrayList<Object>();

	@Override
	public void initialize(Configuration entries, Properties tableProperties) throws SerDeException
	{
		logger.debug("Initializing SerDe");
		// Get column names and sort order
		String columnNameProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
		String columnTypeProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);

		logger.debug("columns " + columnNameProperty + " types " + columnTypeProperty);

		// all table column names
		if (columnNameProperty.length() == 0) {
			columnNames = new ArrayList<>();
		} else {
			columnNames = Arrays.asList(columnNameProperty.split(","));
		}

		// all column types
		if (columnTypeProperty.length() == 0) {
			columnTypes = new ArrayList<>();
		} else {
			columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
		}
		assert columnNames.size() == columnTypes.size();

		// Create row related objects
		rowTypeInfo = (StructTypeInfo) TypeInfoFactory
				.getStructTypeInfo(columnNames, columnTypes);

		this.objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
	}



	/**
	 * This method does the work of deserializing a record into Java objects that
	 * Hive can work with via the ObjectInspector interface.
	 */
	@Override
	public Object deserialize(Writable blob) throws SerDeException
	{
		Map<?,?> root;
		row.clear();
		try {
			MapBasedInputRow mapBasedInputRow = (MapBasedInputRow) blob;
			logger.debug("log druid segment dimentions");
			for (String colmuns : mapBasedInputRow.getDimensions())
			{
				logger.debug(colmuns);
			}

		} catch (Exception e) {
			throw new SerDeException(e);
		}

//		// Lowercase the keys as expected by hive
//		Map<String, Object> lowerRoot = new HashMap();
//		for(Map.Entry entry: root.entrySet()) {
//			lowerRoot.put(((String)entry.getKey()).toLowerCase(), entry.getValue());
//		}
//		root = lowerRoot;
//
//		Object value= null;
//		for (String fieldName : rowTypeInfo.getAllStructFieldNames()) {
//			try {
//				TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo(fieldName);
//				value = parseField(root.get(fieldName), fieldTypeInfo);
//			} catch (Exception e) {
//				value = null;
//			}
//			row.add(value);
//		}
		return row;
	}

	/**
	 * Return an ObjectInspector for the row of data
	 */
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return objectInspector;
	}
	@Override
	public SerDeStats getSerDeStats()
	{
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass()
	{
		return null;
	}

	@Override
	public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException
	{
		return null;
	}
}
