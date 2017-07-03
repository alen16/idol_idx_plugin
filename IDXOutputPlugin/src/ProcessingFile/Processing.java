package ProcessingFile;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Processing {
	private List<Object[]> records = null;
	private List<String> list = null;
	private Map<String, String[]> map = null;
	private String file = "";
	private String default_encoding = "utf-8";
	private String encoding;
	private String output;
	
	public Processing(String file, String encoding) {
		// TODO Auto-generated constructor stub
		this.file = file;
		this.encoding = encoding;
	}
	
	public Processing(String file) {
		// TODO Auto-generated constructor stub
		this.file = file;
	}
	
	public Processing(){
		
	}
	
	public String getOutput(){
		return this.output;
	}
	
	public List<Object[]> getRecords(){
		return this.records;
	}
	
	//Get fieled list
	public List<String> getList(){
		return this.list;
	}
	
	//Get field Map
	public Map<String, String[]> getMap(){
		return this.map;
	}
	public void readIDXFileByRecord(int num_record){
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(file)));
			String tmp = br.readLine();
			List<Object[]> records = new ArrayList<Object[]>();
			List<String> list = new ArrayList<String>();
			Map<String, String[]> FieldMap= new HashMap<String, String[]>();
			int record_count=0;
			int num_fields=0;
			System.out.println("#records:"+record_count);
			while(tmp!=null && record_count<num_record){
				if(!tmp.isEmpty()){
					
					if(tmp.contains("#DRECONTENT")){
						//Concatenate values between DRECONTENT and DREDOCUMENT
						String content = "";
						tmp=br.readLine();
						while(!tmp.contains("#DREENDDOC")){							
							content+=tmp+"\n";
							tmp=br.readLine();
						}		
						byte[] value = content.getBytes(default_encoding);
								
						String fieldType = "DRECONTENT";

						String[] str = new String[3];
						str[0]="DRECONTENT";
						str[1]="DRECONTENT";
						str[2]=new String(value, encoding);
						FieldMap.put(fieldType, str);
						list.add(num_fields, "DRECONTENT");
						num_fields=0;
					}else{
						
						if(tmp.contains("=")){
							//Processe DREFIELD
							String[] tokens = tmp.split("=");
							byte[] value = tokens[1].replace("\"", "").getBytes(default_encoding);
							String fieldName = tokens[0].replace("#DREFIELD ", "");
							String fieldVal1 = new String(value, encoding);				
							if(FieldMap.containsKey(fieldName)){
								String[] str = FieldMap.get(fieldName);
								str[2]+="^"+fieldVal1;
								FieldMap.put(fieldName, str);								
							}else{								
								String[] str = new String[3];
								str[0]="DREFIELD";
								str[1]=fieldName;
								str[2]=fieldVal1;
								FieldMap.put(fieldName, str);
								list.add(num_fields, fieldName);
								num_fields++;
							}
							/*
							String[] str = new String[3];
							str[0]="DREFIELD";
							str[1]=tokens[0].replace("#DREFIELD ", "");
							str[2]=new String(value, "UTF-8");
							list.add(str);
							*/
						}else{
							
							//Processe OTHER fields
							String[] tokens = tmp.split("\\s");
							try{
								//byte[] value = tokens[1].getBytes("UTF-8");
								//row_map.put(tokens[0].replace("#", ""), tokens[1]);
								String fieldType = tokens[0].replace("#", "");
								byte[] value = tokens[1].getBytes(default_encoding);
								String fieldValue = new String(value, encoding);

								if(FieldMap.containsKey(fieldType)){
									String[] str = FieldMap.get(fieldType);
									str[2]+="^"+fieldValue;
									FieldMap.put(fieldType, str);								
								}else{								
									String[] str = new String[3];
									str[0]=fieldType;
									str[1]=fieldType;
									str[2]=fieldValue;
									FieldMap.put(fieldType, str);
									list.add(num_fields, fieldType);
									num_fields++;
								}
							}catch(ArrayIndexOutOfBoundsException aio){
								System.err.println("AIO caused by"+ tmp);
							}
							
						}
					}
					
					if(tmp.contains("DREENDDOC")){
						Object[] obj = new Object[2];
						obj[0] = FieldMap;
						obj[1] = list;
						records.add(record_count, obj);
						//Initial record
						record_count++;
						//list= new ArrayList<String[]>();
						FieldMap= new HashMap<String, String[]>();
						list = new ArrayList<String>();
						System.out.println("#records:"+record_count);
					}
				}
				tmp = br.readLine();
			}
			br.close();
			this.records = records;

		}catch(IOException io){
			io.printStackTrace();
		}
	}
	
	public void readIDXFile(){
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(file)));
			String tmp = br.readLine();
			this.output="";
			while(tmp!=null){
				output+=tmp+"\n";
				tmp = br.readLine();
			}
			br.close();			

		}catch(IOException io){
			io.printStackTrace();
		}
	}
	
	public void print() throws UnsupportedEncodingException{
		for(int i=0;i<records.size();i++){
			Object[] obj = records.get(i);
			Map<String, String[]> map=null;
			List<String> list = null;
			if(obj[0] instanceof Map<?, ?>){
				 map= (Map<String, String[]>) obj[0];
			}
			if(obj[1] instanceof List<?>){
				 list= (List<String>) obj[1];
			}
			if(map != null && list != null){
				Set<String> keySet = map.keySet();
				for(int j=0;j<list.size();j++){
					String[] str=map.get(list.get(j));
					System.out.println(j+"\t"+str[0]+"\t"+str[1]+"\t"+str[2]);
				}
			}
		}
	}
	

	
	public static void main(String[] args) throws UnsupportedEncodingException{
		// TODO Auto-generated method stub
		String file = "E:/idx input/template/tb_idol_bizexchange.idx";
		Processing proc = new Processing(file, "UTF-8");
		//proc.readIDXFileByRecord(1);
		//proc.print();
		proc.readIDXFile();
		System.out.println(proc.output);
	}

}
