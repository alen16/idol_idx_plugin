package ProcessingFile;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegularExpression {
	private String line;
	private String pattern="(<!--)(\\w+)(-->)";
	//private List<String> outputList;
	private String originalStr;
	private String fieldName;
	
	public RegularExpression(String line){
		this.line = line;
	}
	
	public String getOriginalStr(){
		return this.originalStr;
	}
	
	public String getFieldName(){
		return this.fieldName;
	}
	
	public boolean regularExpression(){
		  // Create a Pattern object
	      Pattern r = Pattern.compile(pattern);

	      // Now create matcher object.
	      Matcher m = r.matcher(line);
	      
	    boolean matchFound = m.find();
	    //this.outputList = new ArrayList<String>();
	    if(matchFound){
		    this.originalStr = m.group(0);
		    this.fieldName = m.group(2);
	    }
	    /*
	    while(matchFound) {
	      //System.out.println(m.start() + "-" + m.end());
	      for(int i = 0; i <= m.groupCount(); i++) {
	        String groupStr = m.group(i);
	        //System.out.println(i + ":" + groupStr);
	        this.outputList.add(groupStr);
	      }
	      if(m.end() + 1 <= line.length()) {
	        matchFound = m.find(m.end());
	      }else{
	        break;
	      }
	    }*/
	    return matchFound;
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException{
		// TODO Auto-generated method stub

	      String line = "#DREREFERENCE TB_IDOL_BIZEXCHANGE_<!--ROWNUM-->";
	      RegularExpression reg = new RegularExpression(line);
	      
	      reg.regularExpression();
	      System.out.println(reg.getOriginalStr()+" "+reg.getFieldName());
	      /*
	      List<String> outputList = reg.getOutputList();
	      if(outputList.contains("ROWNUM")){
	    	  System.out.println(outputList.indexOf("ROWNUM"));
	      }*/
	      
	      
	      
	}


}
