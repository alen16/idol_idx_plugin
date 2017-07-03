package be.ibridge.kettle.idxoutput;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import ProcessingFile.Processing;
import ProcessingFile.RegularExpression;


public class IDXOutputPlugin extends BaseStep implements StepInterface{
	private IDXOutputPluginData data;
	private IDXOutputPluginMeta meta;
	private String template;
	private String[] tokens;
	private String previoud_ID = "";
	private boolean firstRecord=true;
	private Map<String, Set<String>> map = new HashMap<String, Set<String>>();
	private String merge = "|";
	private String regex = "\\|";
	
	public IDXOutputPlugin(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis)
	{
		super(s,stepDataInterface,c,t,dis);
	}
	
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	{
	    meta = (IDXOutputPluginMeta)smi;
	    data = (IDXOutputPluginData)sdi;
	    
	    //boolean result = true;
	    boolean bEndedLineWrote = false;
		Object[] r=getRow();    // get row, blocks when needed!
		       
        if (r!=null && first) {
        	first = false;
            data.outputRowMeta = (RowMetaInterface)getInputRowMeta().clone();
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this);            
            //TODO: add some code to read the data from previous step
            logBasic("Template file Name: "+meta.getTemplateFileName());
            if(!meta.getTemplateFileName().isEmpty()){
            	Processing proc = new Processing(meta.getTemplateFileName());
            	proc.readIDXFile();
            	//logBasic(proc.getOutput());
            	this.template = proc.getOutput();
            	this.tokens = template.split("\n");
            }            
            
            File template = new File(meta.getTemplateFileName());
            String folder= template.getName();
            folder = folder.replace(".template", "");
            logBasic("IDX ouput path:"+folder);
            
            File source = new File("idx/"+folder);
            if(!source.exists()){
            	source.mkdir();
            }
            //String parent = file.getParent();
            if(meta.isFileNameInfield()){
            	String fileName = getFileNameFromField(r);
            	meta.setFileName("idx/"+folder+"/"+ fileName );
            	
            }
            
            logBasic("Output file Name: "+meta.getFileName());
            //String fileName = meta.getFileName();
            String ext = "";
            //Set output file format by Allen 2015/3/1
            if(meta.getFileFormat().equals("GZIP")){
            	ext = ".idx.gz";
            	meta.setFileName(meta.getFileName()+ext);
            }else if(meta.getFileFormat().equals("IDX")){
            	ext = ".idx";
            	meta.setFileName(meta.getFileName()+ext);
            }
            //Output file
            /*
            boolean isFileExist = checkIsFileExist(meta.getFileName());
            String modifiedFileName = meta.getFileName();
            while(isFileExist){
            	modifiedFileName = modifyFileName(modifiedFileName, fileName, ext); //Full_FileName, FileName, extension
            	isFileExist = checkIsFileExist(modifiedFileName);
            }
            meta.setFileName(modifiedFileName);
            */
            
            File file = new File(meta.getFileName());
            
        	if(file.exists()){
        		//String modifiedFileName = modifyFileName(meta.getFileName(), fileName, ext); //Full_FileName, FileName, extension
        		
        		//file = new File(meta.getFileName());
        		file.delete();
        	}
        	
        	
            if(!meta.getFileName().isEmpty()){
	            openNewFile(meta.getFileName());
	            data.oneFileOpened = true;
	            initBinaryDataFields();
            }
            
            data.fieldnrs = new int[meta.getOutputField().length];
            for (int i = 0; i < meta.getOutputField().length; i++) {
				data.fieldnrs[i] = data.outputRowMeta.indexOfValue(meta.getOutputField()[i]);
				if (data.fieldnrs[i] < 0) {
					throw new KettleStepException("Field [" + meta.getOutputField()[i] + "] couldn't be found in the input stream!");
				}
			}
            writeEndedLine();
        }
        
        if (r==null)  // no more input to be expected...
		{
			if (false == bEndedLineWrote) {
				// add tag to last line if needed
				if(map !=null){
					writeRecord(map); //寫入舊的record
				}
				writeEndedLine();
				bEndedLineWrote = true;
			}
			setOutputDone();
			closeFile();
			return false;
		}else{
			//openNewFile(meta.getFileName());
			
			writeRowToFile(data.outputRowMeta, r);
			if (data.outputRowMeta != null){
				incrementLinesOutput();
			}		
		}
        
        //writeRowToFile(data.outputRowMeta, r);
        
        //Object extraValue = meta.getValue().getValueData();
        
        //Object[] outputRow = RowDataUtil.addValueData(r, data.outputRowMeta.size()-1, extraValue);
		
		putRow(data.outputRowMeta, r);     // copy row to possible alternate rowset(s).
		
		if (checkFeedback(getLinesRead())) {
			logBasic("Linenr " + getLinesRead()); // Some basic logging
		}
		
		return true;
	}
	
	public boolean checkIsFileExist(String fileName){
		File file = new File(fileName);
    	if(file.exists()){
    		return true;
    	}
    	return false;
	}
	
	public String modifyFileName(String orig_fileName, String fileName, String ext){
		String modify_fileName ="";
		String num = orig_fileName.replace(ext, "").replace(fileName, "").replace("_", "");
		if(num.equals("")){
			modify_fileName = fileName+"_1"+ext;
		}else{
			int orig_num = Integer.parseInt(num);
			orig_num+=1;
			modify_fileName = fileName+"_"+orig_num+ext;
		}
		
		return modify_fileName;
	}
	
	private void initBinaryDataFields() throws KettleException {
		try {
			data.hasEncoding = !Const.isEmpty(meta.getFileEncoding());
			data.isGZIPFormat = meta.getFileFormat().equals("GZIP")?true:false;
			data.binarySeparator = new byte[] {};
			data.binaryEnclosure = new byte[] {};
			data.binaryNewline   = new byte[] {};
			
			
			data.binarySeparator= environmentSubstitute(",").getBytes(meta.getFileEncoding());
			data.binaryEnclosure = environmentSubstitute("\"").getBytes(meta.getFileEncoding());
			data.binaryNewline   = "\n".getBytes(meta.getFileEncoding());
			
		}
		catch(Exception e) {
			throw new KettleException("Unexpected error while encoding binary fields", e);
		}
	}
	
	public boolean init(StepMetaInterface smi, StepDataInterface sdi)
	{
	    meta = (IDXOutputPluginMeta)smi;
	    data = (IDXOutputPluginData)sdi;

	    return super.init(smi, sdi);
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi)
	{
	    meta = (IDXOutputPluginMeta)smi;
	    data = (IDXOutputPluginData)sdi;

	          if (data.oneFileOpened)
	            closeFile();
	    
	          try {
	            if (data.fos != null) {
	              data.fos.close();
	            }
	          } catch (Exception e) {
	            logError("Unexpected error closing file", e);
	            setErrors(1);
	         }
	        
	    super.dispose(smi, sdi);
	}
	
	//
	// Run is were the action happens!
	public void run()
	{
		logBasic("Starting to run...");
		try
		{
			while (processRow(meta, data) && !isStopped());
		}
		catch(Exception e)
		{
			logError("Unexpected error : "+e.toString());
            logError(Const.getStackTracker(e));
			setErrors(1);
			stopAll();
		}
		finally
		{
		    dispose(meta, data);
			logBasic("Finished, processing "+getLinesRead() +" rows");
			markStop();
		}
	}
	


	private void writeRowToFile(RowMetaInterface rowMeta, Object[] r) throws KettleStepException {
		try	{
			//Write rows to file
			if (meta.getOutputField()!=null) {
				String id = getMatchIDFromField(r); //取得matchID, 通常都是CompanyID, productID等等
				logBasic("previous_ID:"+previoud_ID+" incomming id:"+id);
				if(previoud_ID.equals(id)){
					//如果舊id == 這筆record id
					
					mergeRecord(rowMeta, r); //執行merge動作
					
				}else{
					System.out.println("map"+map);
					if(!firstRecord){
						writeRecord(map); //寫入舊的record
					}
					//Create a new map for new ID.
					map = null;
					map = new HashMap<String, Set<String>>(); //重開新的map 儲存新的record

					writeRowToMap(rowMeta, r); //執行write動作
					firstRecord=false;
					previoud_ID=id;
				}				
			}		
		}catch(Exception e){
			throw new KettleStepException("Error writing line", e);
		}
	}
	
	private void writeRowToMap(RowMetaInterface rowMeta, Object[] r) throws KettleStepException {
		try	{
			//Merge rows to map
			if (meta.getOutputField()!=null){
				
				/*
				 * Only write the fields specified!
				 */
				
				for (int i=0;i<meta.getOutputField().length;i++) {
	
					//ValueMetaInterface v = rowMeta.getValueMeta(data.fieldnrs[i]);
					Object valueData = r[data.fieldnrs[i]];
					
					String fieldValue = "";
					String fieldName = meta.getOutputField()[i];

					if(valueData != null){
						fieldValue = valueData.toString();						

						//logBasic("keyfield: "+keyfield[i]+" fieldName: "+fieldName+" valueData: "+fieldValue);
						if(!fieldValue.equals("")){
							Set<String> set = new HashSet<String>();
							set.add(fieldValue);
							map.put(fieldName, set); //Save rows of given record.
						}

					}
				}
			}
		} catch(Exception e) {
			throw new KettleStepException("Error writing line", e);
		}
	}
	
	private void mergeRecord(RowMetaInterface rowMeta, Object[] r) throws KettleStepException {
		try	{
			//Merge rows to map
			if (meta.getOutputField()!=null){
				
				/*
				 * Only write the fields specified!
				 */
				String[] keyfield = meta.getKeyField();
				
				for (int i=0;i<meta.getOutputField().length;i++) {
	
					//ValueMetaInterface v = rowMeta.getValueMeta(data.fieldnrs[i]);
					Object valueData = r[data.fieldnrs[i]];
					String fieldValue = "";
					String fieldName = meta.getOutputField()[i];

					if(valueData != null){
						fieldValue = valueData.toString();
							//Merge values with specified keyfield
							if(map.containsKey(fieldName)){ //如果map中含有此fieldName，則執行合併
								Set<String> oldfieldValue = map.get(fieldName); //先取得舊的fieldValue
								if(keyfield[i]!= null){ //當keyfield=1，才需要合併
									/*
									if(!oldfieldValue.equals("")){ //如果舊的fieldValue不為空
										oldfieldValue+=merge+fieldValue; //執行合併
									}else{ 
										//如果舊的fieldValue為空
										if(!fieldValue.equals("")){ //則當fieldValue不為空時
											oldfieldValue+=fieldValue; //才加入至fieldValue中
										}
									}
									*/
									oldfieldValue.add(fieldValue);
									map.put(fieldName, oldfieldValue);
								}
									
							}else{
								if(!fieldValue.equals("")){
									Set<String> set = new HashSet<String>();
									set.add(fieldValue);
									map.put(fieldName, set); //Save rows of given record.
								}
							}
					}
				}
			}
		} catch(Exception e) {
			throw new KettleStepException("Error writing line", e);
		}
	}
	
	public String getFileNameFromField(Object[] r) throws KettleStepException {
		String fileName = meta.getFileNameField();
		try	{

        	// find and set index of file name field in input stream
            //
        	data.fileNameFieldIndex = getInputRowMeta().indexOfValue( meta.getFileNameField() );
        	
        	 // set the file name for this row
            //
            if ( data.fileNameFieldIndex < 0 ) {
              throw new KettleStepException( "FileNameFieldNotFound:"+ meta.getFileNameField() );
            }

            data.fileNameMeta = getInputRowMeta().getValueMeta( data.fileNameFieldIndex );
            fileName = data.fileNameMeta.getString( r[data.fileNameFieldIndex] );
            
		} catch(Exception e) {
			throw new KettleStepException("Error get filename from field", e);
		}
		return fileName;
	}
	
	public String getMatchIDFromField(Object[] r) throws KettleStepException {
		String fileName = meta.getFileNameField();
		try	{

        	// find and set index of file name field in input stream
            //
        	data.matchIDFieldIndex = getInputRowMeta().indexOfValue( meta.getMatchIDField() );
        	 // set the file name for this row
            //
            if ( data.matchIDFieldIndex < 0 ) {
              throw new KettleStepException( "MatchIDNotFound:"+ meta.getFileNameField() );
            }

            data.matchIDMeta = getInputRowMeta().getValueMeta( data.matchIDFieldIndex );
            fileName = data.matchIDMeta.getString( r[data.matchIDFieldIndex] );
            
		} catch(Exception e) {
			throw new KettleStepException("Error get matchID from field", e);
		}
		return fileName;
	}
	
	private void writeEndDoc() {

		try{
			String sLine = "#DREENDDOC";
			if (sLine!=null) {
				byte[] fieldNameToByte = getBinaryString(sLine);
				data.writer.write(fieldNameToByte);
				data.writer.write(data.binaryNewline);
			}
		}catch(Exception e){
			logError("Error writing end dreenddoc line: "+e.toString());
		}
	}
	
	private boolean writeEndedLine() {
		boolean retval=false;
		try
		{
			String sLine = "";
			if (sLine!=null)
			{
				if (sLine.trim().length()>0)
				{
					data.writer.write(getBinaryString(sLine));
					incrementLinesOutput();
				}
			}
		}
		catch(Exception e)
		{
			logError("Error writing ended tag line: "+e.toString());
            logError(Const.getStackTracker(e));
			retval=true;
		}
		
		return retval;
	}
	
	 private void writeRecord(Map<String, Set<String>> map) throws KettleStepException {
	        try {
	        	// First check whether or not we have a null string set
	        	// These values should be set when a null value passes
	        		        	
	        	RegularExpression reg = null;
	        	if(tokens!=null){
		        	for(int i=0;i<tokens.length;i++){
		        		//Iterators all template line
		        		reg = new RegularExpression(tokens[i]);
		        		boolean matchFound = reg.regularExpression();
		        		if(matchFound){
			        		String originalStr = reg.getOriginalStr(); //ex. "<!--CID-->"
			        		String fieldName = reg.getFieldName(); //ex. CID
			                
			        		//logBasic("keyfield:"+keyfield+" fieldName:"+fieldName+" Value:"+map.containsKey(fieldName));
			        		if(map.containsKey(fieldName)){
			        			Set<String> set = map.get(fieldName);
			        			String value="";
			        			int count=0; //紀錄合併次數
			        			//如果多值，則以 |合併
			        			for(String s: set){
			        				if(count==0){
			        					value+=s;
			        				}else{
			        					value+=this.merge+s;
			        				}
			        				count++;
			        			}
			        			
			        			if(count<=1){
			        				//針對非組合欄位，列印其值
			        				if(value.contains(merge)){
			        					//先印一次
					        			String new_line = tokens[i].replace("=", "_COMBO=").replace(originalStr, value); //ex. "<!--CID-->" replaced by "101"
					        			byte[] fieldNameToByte = getBinaryString(new_line.replaceAll("\n", " "));
					        			data.writer.write(fieldNameToByte);
						            	data.writer.write(data.binaryNewline);
			        				}else{
					        			String new_line = tokens[i].replace(originalStr, value); //ex. "<!--CID-->" replaced by "101"
					        			//logBasic(new_line.replaceAll("\n", " "));
					        			byte[] fieldNameToByte = getBinaryString(new_line.replaceAll("\n", " "));
					        			data.writer.write(fieldNameToByte);
						            	data.writer.write(data.binaryNewline);
			        				}
			        			}else{
			        				//針對組合欄位，取代欄位名稱為_COMBO結尾後，列印其值
				        			String new_line = tokens[i].replace("=", "_COMBO=").replace(originalStr, value); //ex. "<!--CID-->" replaced by "101"
				        			//logBasic(new_line.replaceAll("\n", " "));
				        			byte[] fieldNameToByte = getBinaryString(new_line.replaceAll("\n", " "));
				        			data.writer.write(fieldNameToByte);
					            	data.writer.write(data.binaryNewline);
			        			}
			        			//判斷是否為合併值
				            	if(value.contains(merge)){
				        			String[] values = value.split(regex);
				        			//logBasic(new_line.replaceAll("\n", " "));


				        				//如果是合併值，則每個值在印一次
				        				for(String v: values){
				        					if(!v.equals("")){
					        					String new_line = tokens[i].replace(originalStr, v); //ex. "<!--CID-->" replaced by "101"
							        			//logBasic(new_line.replaceAll("\n", " "));
					        					byte[] fieldNameToByte = getBinaryString(new_line.replaceAll("\n", " "));
							        			data.writer.write(fieldNameToByte);
								            	data.writer.write(data.binaryNewline);
				        					}
				        				}

				            	}
				            	
			        			
			        		}
		        		} else {
		        			//handle non regular expression pattern, ex. #DREFIELD LANGUAGETYPE="englishUTF8", #DREENDDOC
		        			byte[] fieldNameToByte = getBinaryString(tokens[i]);
		        			data.writer.write(fieldNameToByte);
			            	data.writer.write(data.binaryNewline);
		        		}
		        	}
		        	//Write #DREENDDOC to the end of record
	        	}
				writeEndDoc();
				if(data.binaryNewline!=null){
					data.writer.write(data.binaryNewline);
				}
				
				incrementLinesOutput();
	        } catch(Exception e) {
	            throw new KettleStepException("Error writing field content to file", e);
	        }
	}	 
	 
	private byte[] getBinaryString(String string) throws KettleStepException {
    	try {
    		if (data.hasEncoding) {
        		return string.getBytes(meta.getFileEncoding());
    		} else {
        		return string.getBytes();
    		}
    	} catch(Exception e) {
    		throw new KettleStepException(e);
    	}
    }
	
	public void openNewFile(String baseFilename) throws KettleException
	{
	  if(baseFilename == null) {
	    throw new KettleFileException("IDXFileOutput.Exception.FileNameNotSet"); //$NON-NLS-1$
	  }

	  data.writer=null;
		
		ResultFile resultFile = null;
		
		String filename = meta.getFileName();
		
		try
		{

            	// Add this to the result file names...
				resultFile = new ResultFile(ResultFile.FILE_TYPE_GENERAL, KettleVFS.getFileObject(filename, getTransMeta()), getTransMeta().getName(), getStepname());
				resultFile.setComment("This file was created with a text file output step");
	            addResultFile(resultFile);

	            OutputStream outputStream;
                

					if(log.isDetailed()) logDetailed("Opening output stream in nocompress mode");
      
					data.fos = KettleVFS.getOutputStream(filename, getTransMeta(), true);

                    outputStream=data.fos;
    
                
	            if (!Const.isEmpty(meta.getFileEncoding()))
	            {
	                if(log.isDetailed()) logDetailed("Opening output stream in encoding: "+meta.getFileEncoding());
	                //Set file format by Allen 2015/3/1
	                if(meta.getFileFormat().equals("GZIP")){
	                	data.writer = new GZIPOutputStream(outputStream);	
	                }else{
	                	data.writer = new BufferedOutputStream(outputStream, 5000);
	                }
	                
	            }
	            else
	            {
	                if(log.isDetailed()) logDetailed("Opening output stream in default encoding");
	              //Set file format by Allen 2015/3/1
	                if(meta.getFileFormat().equals("GZIP")){
	                	data.writer = new GZIPOutputStream(outputStream);	
	                }else{
	                	data.writer = new BufferedOutputStream(outputStream, 5000);
	                }
	            }
	
	            if(log.isDetailed()) logDetailed("Opened new file with name ["+filename+"]");
            
		}
		catch(Exception e)
		{
			throw new KettleException("Error opening new file : "+e.toString());
		}
		// System.out.println("end of newFile(), splitnr="+splitnr);

		data.splitnr++;

        if(resultFile!=null )
        {
			// Add this to the result file names...
            addResultFile(resultFile);
        }
	}
	 
	 private boolean closeFile()
		{
			boolean retval=false;
			
			try
			{			
				if ( data.writer != null )
				{
					if(log.isDebug()) logDebug("Closing output stream");
				    data.writer.close();
				    if(log.isDebug()) logDebug("Closed output stream");
				}			
				data.writer = null;
				if (data.cmdProc != null)
				{
					if(log.isDebug()) logDebug("Ending running external command");
					int procStatus = data.cmdProc.waitFor();
					// close the streams
					// otherwise you get "Too many open files, java.io.IOException" after a lot of iterations
					try {
						data.cmdProc.getErrorStream().close();
						data.cmdProc.getOutputStream().close();				
						data.cmdProc.getInputStream().close();
					} catch (IOException e) {
						if(log.isDetailed()) logDetailed("Warning: Error closing streams: " + e.getMessage());
					}				
					data.cmdProc = null;
					if(log.isBasic() && procStatus != 0) logBasic("Command exit status: " + procStatus);
				}
				else
				{
					if(log.isDebug()) logDebug("Closing normal file ...");
					
	                if (data.fos!=null)
	                {
	                	
	                    data.fos.close();
	                    data.fos=null;
	                    if(data.isGZIPFormat){
	                    	logBasic("Write DREENDDATA to GZIP file.");
	                    	writeEndData();
	                    }
	                }
				}
				
				
				retval=true;
			}
			catch(Exception e)
			{
				logError("Exception trying to close file: " + e.toString());
				setErrors(1);
				retval = false;
			}

			return retval;
		}
	 
	 public void writeEndData(){
		try{
			
				InputStream fis = new FileInputStream(meta.getFileName());
				List<Integer> list = new ArrayList<Integer>();
				int data = fis.read();
				while(data!=-1){
					list.add(data);
					data = fis.read();
				}
				fis.close();
				
				FileOutputStream fos = new FileOutputStream(meta.getFileName());
				for(int i=0;i<list.size();i++){
					fos.write(list.get(i));
				}				
				byte[] dreenddata = "#DREENDDATA".getBytes(meta.getFileEncoding()); 
				fos.write(dreenddata);
				fos.close();
		
		}catch(IOException io){
			io.printStackTrace();
		}
	 }
	 
	 
}
