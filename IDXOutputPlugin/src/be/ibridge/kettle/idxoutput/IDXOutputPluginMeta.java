package be.ibridge.kettle.idxoutput;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

public class IDXOutputPluginMeta extends BaseStepMeta implements StepMetaInterface {
	private static Class<?> PKG = IDXOutputPluginMeta.class; // for IDX purposes
	private String fileFormat;
	private String fileEncoding;
	private String fileName;
	private String templateFileName;
	private boolean fileNameInfield;
	private String fileNameField;
	private String matchIDField;
	private String keyField[];      
	private String outputField[];          
	private String outputDefault[];    
	private int	outputType[];   	
	private String outputFormat[];
	private String outputCurrency[];
	private String outputDecimal[];
	private String outputGroup[];
	private int outputLength[];
	private int outputPrecision[];
	public IDXOutputPluginMeta() {
		// TODO Auto-generated constructor stub
		super();
	}

	
	public String getFileEncoding() {
		return fileEncoding;
	}
	
	public void setFileEncoding(String fileEncoding) {
		this.fileEncoding = fileEncoding;
	}
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public String getTemplateFileName() {
		return templateFileName;
	}

	public void setTemplateFileName(String templateFileName) {
		this.templateFileName = templateFileName;
	}
	
	public String getFileFormat(){
		return fileFormat;
	}
	
	public void setFileFormat(String fileFormat){
		this.fileFormat = fileFormat;
	}
	
	public String[] getKeyField() {
		return keyField;
	}

	public void setKeyField(String[] keyField) {
		this.keyField = keyField;
	}

	public String[] getOutputField() {
		return outputField;
	}

	public void setOutputField(String[] outputField) {
		this.outputField = outputField;
	}

	public String[] getOutputDefault() {
		return outputDefault;
	}

	public void setOutputDefault(String[] outputDefault) {
		this.outputDefault = outputDefault;
	}

	public int[] getOutputType() {
		return outputType;
	}

	public void setOutputType(int[] outputType) {
		this.outputType = outputType;
	}

	public String[] getOutputFormat() {
		return outputFormat;
	}

	public void setOutputFormat(String[] outputFormat) {
		this.outputFormat = outputFormat;
	}

	public String[] getOutputCurrency() {
		return outputCurrency;
	}

	public void setOutputCurrency(String[] outputCurrency) {
		this.outputCurrency = outputCurrency;
	}

	public String[] getOutputDecimal() {
		return outputDecimal;
	}

	public void setOutputDecimal(String[] outputDecimal) {
		this.outputDecimal = outputDecimal;
	}

	public String[] getOutputGroup() {
		return outputGroup;
	}

	public void setOutputGroup(String[] outputGroup) {
		this.outputGroup = outputGroup;
	}

	public int[] getOutputLength() {
		return outputLength;
	}

	public void setOutputLength(int[] outputLength) {
		this.outputLength = outputLength;
	}

	public int[] getOutputPrecision() {
		return outputPrecision;
	}

	public void setOutputPrecision(int[] outputPrecision) {
		this.outputPrecision = outputPrecision;
	}
	
	// set sensible defaults for a new step
	public void setDefault(){
		/*
		value = new ValueMetaAndData( new ValueMeta("UTF-8", ValueMetaInterface.TYPE_NUMBER), new Double(123.456) );
		value.getValueMeta().setLength(12);
        value.getValueMeta().setPrecision(4);
        */
		fileFormat = "IDX";
		fileEncoding = "UTF-8";
		templateFileName = "Please select the template file.";
        fileName = "Please specify the output file.";
        allocate(0);
	}
	
	// helper method to allocate the arrays
	public void allocate(int nrkeys){
			
		keyField			= new String[nrkeys];
		outputField			= new String[nrkeys];
		outputDefault		= new String[nrkeys];
		outputType			= new int[nrkeys];
		outputFormat		= new String[nrkeys];
		outputDecimal		= new String[nrkeys];
		outputGroup			= new String[nrkeys];
		
		outputLength		= new int[nrkeys];
		
		outputPrecision		= new int[nrkeys];
		outputCurrency 		= new String[nrkeys];
			
	}
	
	public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space)
	{
		/*
		if (value!=null)
		{
			ValueMetaInterface v = value.getValueMeta();
			v.setOrigin(origin);
			
			r.addValueMeta( v );
		}
		*/
		
		// append the outputFields to the output
		for (int i=0;i<outputField.length;i++){
			ValueMetaInterface v=new ValueMeta(outputField[i], outputType[i]);
			v.setLength(outputLength[i]);
	        v.setPrecision(outputPrecision[i]);
	        v.setCurrencySymbol(outputCurrency[i]);
	        v.setConversionMask(outputFormat[i]);
	        v.setDecimalSymbol(outputDecimal[i]);
	        v.setGroupingSymbol(outputGroup[i]);
		           
			v.setOrigin(origin);
			r.addValueMeta(v);
		}
	}
	
	public Object clone(){
		IDXOutputPluginMeta retval = (IDXOutputPluginMeta) super.clone();
		// add proper deep copy for the collections
		
		int nrKeys   = keyField.length;

		retval.allocate(nrKeys);
				
		for (int i=0;i<nrKeys;i++){
			retval.keyField[i] = keyField[i];
			retval.outputField[i] = outputField[i];
			retval.outputDefault[i] = outputDefault[i];
			retval.outputType[i] = outputType[i];
			retval.outputCurrency[i] = outputCurrency[i];
			retval.outputDecimal[i] = outputDecimal[i];
			retval.outputFormat[i] = outputFormat[i];
			retval.outputGroup[i] = outputGroup[i];
			retval.outputLength[i] = outputLength[i];
			retval.outputPrecision[i] = outputPrecision[i];
		}
				
		return retval;
	}
		
	public String getXML() throws KettleException {
		
		StringBuffer retval = new StringBuffer(150);
			
		retval.append("    ").append(XMLHandler.addTagValue("FileEncoding", fileEncoding));
		retval.append("    ").append(XMLHandler.addTagValue("TemplateFileName", templateFileName));
		retval.append("    ").append(XMLHandler.addTagValue("FileName", fileName));
		retval.append( "    " + XMLHandler.addTagValue( "matchIDField", matchIDField ) );
		retval.append( "    " + XMLHandler.addTagValue( "fileNameInField", fileNameInfield ) );
	    retval.append( "    " + XMLHandler.addTagValue( "fileNameField", fileNameField ) );
	    retval.append("    ").append(XMLHandler.addTagValue("format", fileFormat));
	    
		for (int i=0;i<keyField.length;i++)
		{
			retval.append("      <field>").append(Const.CR);
			retval.append("        ").append(XMLHandler.addTagValue("keyfield", keyField[i]));
			retval.append("        ").append(XMLHandler.addTagValue("outfield", outputField[i]));
			retval.append("        ").append(XMLHandler.addTagValue("default", outputDefault[i]));
			retval.append("        ").append(XMLHandler.addTagValue("type", ValueMeta.getTypeDesc(outputType[i])));
			retval.append("        ").append(XMLHandler.addTagValue("format", outputFormat[i]));
			retval.append("        ").append(XMLHandler.addTagValue("decimal", outputDecimal[i]));
			retval.append("        ").append(XMLHandler.addTagValue("group", outputGroup[i]));
			retval.append("        ").append(XMLHandler.addTagValue("length", outputLength[i]));
			retval.append("        ").append(XMLHandler.addTagValue("precision", outputPrecision[i]));
			retval.append("        ").append(XMLHandler.addTagValue("currency", outputCurrency[i]));
			
			retval.append("      </field>").append(Const.CR);
		}
		//The following code is for Debug
		
		try{
			PrintWriter pw = new PrintWriter(new FileOutputStream(new File("E:/IDX_output.log")));
			pw.println(retval.toString());
			pw.close();
		}catch(IOException io){
			io.printStackTrace();
		}
		
		return retval.toString();
	}

	

	

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String,Counter> counters) throws KettleXMLException{
		try {
			//影響儲存後，以及資料傳送時，是否仍能取得值
			fileEncoding = XMLHandler.getTagValue(stepnode, "FileEncoding");
			templateFileName = XMLHandler.getTagValue(stepnode, "TemplateFileName"); 
			fileName = XMLHandler.getTagValue(stepnode, "FileName");
			matchIDField = XMLHandler.getTagValue( stepnode, "matchIDField" );
			fileNameInfield = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "fileNameInField" ) );
		    fileNameField = XMLHandler.getTagValue( stepnode, "fileNameField" );
			fileFormat = XMLHandler.getTagValue(stepnode, "format");
			
			int nrKeys = XMLHandler.countNodes(stepnode, "field"); 
			allocate(nrKeys);
			
			for (int i=0;i<nrKeys;i++)
			{
				Node knode = XMLHandler.getSubNodeByNr(stepnode, "field", i);
				
				keyField[i] 		= XMLHandler.getTagValue(knode, "keyfield"); 
				outputField[i] 		= XMLHandler.getTagValue(knode, "outfield");
				outputDefault[i] 	= XMLHandler.getTagValue(knode, "default");
				outputType[i] 		= ValueMeta.getType(XMLHandler.getTagValue(knode, "type"));
				outputFormat[i] 	= XMLHandler.getTagValue(knode, "format");
				outputDecimal[i]	= XMLHandler.getTagValue(knode, "decimal");
				outputGroup[i] 		= XMLHandler.getTagValue(knode, "group");
				outputLength[i] 	= Const.toInt(XMLHandler.getTagValue(knode, "length"), -1);
				outputPrecision[i] 	= Const.toInt(XMLHandler.getTagValue(knode, "precision"), -1);
				outputCurrency[i] 	= XMLHandler.getTagValue(knode, "currency");
				
				if (outputType[i]<0){
					outputType[i]=ValueMetaInterface.TYPE_STRING;
				}
				
			}
			
		} catch (Exception e) {
			throw new KettleXMLException("Template Plugin Unable to read step info from XML node", e);
		}
	}

	
	

	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String,Counter> counters) throws KettleException {
		try	{
			fileEncoding = rep.getStepAttributeString(id_step, "FileEncoding");
			templateFileName = rep.getStepAttributeString(id_step, "TemplateFileName");
			fileName = rep.getStepAttributeString(id_step, "FileName");
			matchIDField = rep.getStepAttributeString( id_step, "matchIDField" );
			fileNameInfield = rep.getStepAttributeBoolean( id_step, "fileNameInField" );
		    fileNameField = rep.getStepAttributeString( id_step, "fileNameField" );
			fileFormat = rep.getStepAttributeString(id_step, "format");
			
			int nrKeys   = rep.countNrStepAttributes(id_step, "keyfield");
			allocate(nrKeys);
			
			for (int i=0;i<nrKeys;i++) {
				keyField[i] 		= rep.getStepAttributeString (id_step, i, "keyfield");
				outputField[i] 		= rep.getStepAttributeString (id_step, i, "outfield");
				outputDefault[i] 	= rep.getStepAttributeString (id_step, i, "default");
				outputType[i]	 	= ValueMeta.getType( rep.getStepAttributeString (id_step, i, "type") );
				outputFormat[i] 	= rep.getStepAttributeString (id_step, i, "format");
				outputDecimal[i] 	= rep.getStepAttributeString (id_step, i, "decimal");
				outputGroup[i] 		= rep.getStepAttributeString (id_step, i, "group");
				outputLength[i] 	= Const.toInt(rep.getStepAttributeString (id_step, i, "length"), -1);
				outputPrecision[i] 	= Const.toInt(rep.getStepAttributeString (id_step, i, "precision"), -1);
				outputCurrency[i] 	= rep.getStepAttributeString (id_step, i, "currency");

			}
			
		} catch(Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "IDXOutputStep.Exception.UnexpectedErrorInReadingStepInfo"), e);
		}
	}
	
	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
		try	{
			rep.saveStepAttribute(id_transformation, id_step, "FileEncoding", fileEncoding); 
			rep.saveStepAttribute(id_transformation, id_step, "TemplateFileName", templateFileName); 
			rep.saveStepAttribute(id_transformation, id_step, "FileName", fileName);
			rep.saveStepAttribute( id_transformation, id_step, "matchIDField", matchIDField );
		    rep.saveStepAttribute( id_transformation, id_step, "fileNameInField", fileNameInfield );
		    rep.saveStepAttribute( id_transformation, id_step, "fileNameField", fileNameField );
		    rep.saveStepAttribute(id_transformation, id_step, "format", fileFormat);
		    
			for (int i=0;i<keyField.length;i++)	{
				rep.saveStepAttribute(id_transformation, id_step, i, "keyfield", keyField[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "outfield", outputField[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "default", outputDefault[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "type", ValueMeta.getTypeDesc(outputType[i]));
				rep.saveStepAttribute(id_transformation, id_step, i, "format", outputFormat[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "decimal", outputDecimal[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "group", outputGroup[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "length", outputLength[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "precision", outputPrecision[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "currency", outputCurrency[i]);
				
			}
		} catch(KettleDatabaseException dbe) {
			throw new KettleException("Unable to save step information to the repository, id_step="+id_step, dbe);
		}
	}

	public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info)
	{
		CheckResult cr;

		// See if we have input streams leading to this step!
		if (input.length>0)	{
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta);
			remarks.add(cr);
		} else {
			cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta);
			remarks.add(cr);
		}
		
		// also check that each expected key fields are actually coming
		if (prev!=null && prev.size()>0) {
			boolean first=true;
			String error_message = ""; 
			boolean error_found = false;
			
			for (int i=0;i<keyField.length;i++) {
				ValueMetaInterface v = prev.searchValueMeta(keyField[i]);
				if (v==null) {
					if (first) {
						first=false;
						error_message+=BaseMessages.getString(PKG, "IDXOutputStep.Check.MissingFieldsNotFoundInInput")+Const.CR;
					}
					error_found=true;
					error_message+="\t\t"+keyField[i]+Const.CR;
				}
			}
			if (error_found) {
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
			} else {
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "IDXOutputStep.Check.AllFieldsFoundInInput"), stepMeta);
			}
			remarks.add(cr);
		} else {
			String error_message=BaseMessages.getString(PKG, "IDXOutputStep.Check.CouldNotReadFromPreviousSteps")+Const.CR;
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
			remarks.add(cr);
		}		
	}
	
	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new IDXOutputPluginDialog(shell, meta, transMeta, name);
	}

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {
		return new IDXOutputPlugin(stepMeta, stepDataInterface, cnr, transMeta, disp);
	}

	public StepDataInterface getStepData() {
		return new IDXOutputPluginData();
	}


	public boolean isFileNameInfield() {
		return fileNameInfield;
	}


	public void setFileNameInfield(boolean fileNameInfield) {
		this.fileNameInfield = fileNameInfield;
	}


	public String getFileNameField() {
		return fileNameField;
	}


	public void setFileNameField(String fileNameField) {
		this.fileNameField = fileNameField;
	}


	public String getMatchIDField() {
		return matchIDField;
	}


	public void setMatchIDField(String matchIDField) {
		this.matchIDField = matchIDField;
	}

}
