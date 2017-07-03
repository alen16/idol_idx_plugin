package be.ibridge.kettle.idxoutput;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class IDXOutputPluginData extends BaseStepData implements StepDataInterface {
	public int splitnr;
	public RowMetaInterface outputRowMeta;
	public OutputStream writer;
	public OutputStream fos;
	public boolean isGZIPFormat = false;
	public boolean hasEncoding = true;
	public boolean oneFileOpened;
	
	public Process cmdProc;
	public int fieldnrs[];
	public byte[] binarySeparator;
	public byte[] binaryEnclosure;
	public byte[] binaryNewline;
	
	public byte[] binaryNullValue[];
	public int fileNameFieldIndex;
	public int matchIDFieldIndex;
	public ValueMetaInterface fileNameMeta;
	public ValueMetaInterface matchIDMeta;
	
	public Map<String,OutputStream> fileWriterMap;
	
	public IDXOutputPluginData() {
		// TODO Auto-generated constructor stub
		super();
		cmdProc = null;
	}

}
