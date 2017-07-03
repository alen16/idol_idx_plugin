package be.ibridge.kettle.idxoutput;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

public class IDXOutputPluginDialog extends BaseStepDialog implements StepDialogInterface {
	private static Class<?> PKG = IDXOutputPluginMeta.class; // for i18n purposes
	static int[] data = new int[0];
	private IDXOutputPluginMeta input;

	private Label        fileEncodingLabel;
	private CCombo       fileEncodingText;
	private Label 		 fileNameLabel;
	private Text 		 fileNameText;
	private String 		 fileName;
	private Label 		 templateFileNameLabel;
	private Text 		 templateFileNameText;
	private String 		 templateFileName;
	private Button       templateFileButtonValue;
	private Label        fileFormatLabel;
	private CCombo       fileFormatText;
	private Button       fileButtonValue;
	private Label 		 wlMatchIDInField;
	private Button		 wMatchIDInField;
	private Label		 wlMatchIDField;
	private ComboVar	 wMatchIDField;
	private Label 		 wlFileNameInField;
	private Button		 wFileNameInField;
	private Label		 wlFileNameField;
	private ComboVar	 wFileNameField;

	private FormData     fdlValName, fdValName, fileForm1, fileForm2, fileForm3, fileForm4, fileForm5, fileForm6, fileForm7, fileForm8, fdlFileNameInField, fdFileNameInField, fdlFileNameField, fdFileNameField, fdlMatchIDInField, fdMatchIDField, fdlMatchIDField, fdMatchIDInField;	
	private Text         wValue;
	
	private boolean gotPreviousFields=false;
	private FileDialog   fileDialog;
	private Label        wlKeys;
	private TableView    wKeys;
	private ColumnInfo   fieldColumn = null;
	private RowMetaInterface prevFields = null;
	private Table table;
	public IDXOutputPluginDialog(Shell parent, Object in, TransMeta transMeta, String sname)
	{
		super(parent, (BaseStepMeta)in, transMeta, sname);
		input=(IDXOutputPluginMeta)in;
		
	}
	
	public String open()
	{
		Shell parent = getParent();
		Display display = parent.getDisplay();
		
		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook( shell );
        setShellImage(shell, input);

		ModifyListener lsMod = new ModifyListener() 
		{
			public void modifyText(ModifyEvent e) 
			{
				input.setChanged();
			}
		};
		changed = input.hasChanged();

		FormLayout formLayout = new FormLayout ();
		formLayout.marginWidth  = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(Messages.getString("IDXOutputPluginDialog.Shell.Title")); //$NON-NLS-1$
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname=new Label(shell, SWT.RIGHT);
		wlStepname.setText(Messages.getString("IDXOutputPluginDialog.StepName.Label")); //$NON-NLS-1$
        props.setLook( wlStepname );
		fdlStepname=new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right= new FormAttachment(middle, -margin);
		fdlStepname.top  = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname=new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
        props.setLook( wStepname );
		wStepname.addModifyListener(lsMod);
		fdStepname=new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top  = new FormAttachment(0, margin);
		fdStepname.right= new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);
		
		// File Encoding line
		fileEncodingLabel=new Label(shell, SWT.RIGHT);
		fileEncodingLabel.setText(Messages.getString("IDXOutputPluginDialog.ValueName.Label")); //$NON-NLS-1$
        props.setLook( fileEncodingLabel );
		fdlValName=new FormData();
		fdlValName.left = new FormAttachment(0, 0);
		fdlValName.right= new FormAttachment(middle, -margin);
		fdlValName.top  = new FormAttachment(wStepname, margin);
		fileEncodingLabel.setLayoutData(fdlValName);
		//fileEncodingText=new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		fileEncodingText=new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
		fileEncodingText.setText(Messages.getString("IDXOutputPluginDialog.ValueName.Label")); //$NON-NLS-1$
        props.setLook( fileEncodingText );
        
        String[] encoding_items = {"UTF-8"};
        fileEncodingText.setItems(encoding_items);
        fileEncodingText.addModifyListener(lsMod);
		fdValName=new FormData();
		fdValName.left = new FormAttachment(middle, 0);
		fdValName.right= new FormAttachment(100, 0);
		fdValName.top  = new FormAttachment(wStepname, margin);
		fileEncodingText.setLayoutData(fdValName);
		
		//Input template filename - Value line
				templateFileNameLabel=new Label(shell, SWT.RIGHT);
				templateFileNameLabel.setText("Template IDX file:"); //$NON-NLS-1$
		        props.setLook( templateFileNameLabel );
		        fileForm1=new FormData();
		        fileForm1.left = new FormAttachment(0, 0);
		        fileForm1.right= new FormAttachment(middle, -margin);
		        fileForm1.top  = new FormAttachment(fileEncodingText, margin);
		        templateFileNameLabel.setLayoutData(fileForm1);

				templateFileButtonValue=new Button(shell, SWT.PUSH| SWT.CENTER);
		        props.setLook( templateFileButtonValue );
		        templateFileButtonValue.setText("Select template File"); //$NON-NLS-1$
		        fileForm2 = new FormData();
		        fileForm2.right = new FormAttachment(100, 0);
		        fileForm2.top = new FormAttachment(fileEncodingText, margin);
				templateFileButtonValue.setLayoutData(fileForm2);

				templateFileNameText = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		        props.setLook(templateFileNameText);
		        templateFileNameText.addModifyListener(lsMod);  
		        fileForm3=new FormData();
		        fileForm3.left = new FormAttachment(middle, 0);
		        fileForm3.right= new FormAttachment(templateFileButtonValue, -margin);
		        fileForm3.top  = new FormAttachment(fileEncodingText, margin);
		        templateFileNameText.setLayoutData(fileForm3);

				templateFileButtonValue.addSelectionListener(new SelectionAdapter()
				{
					public void widgetSelected(SelectionEvent arg0)
					{
						  fileDialog=new FileDialog(shell,SWT.OPEN);
					      fileDialog.setText("Please enter template file Name:");
					      templateFileName=fileDialog.open();
					      if (templateFileName != null) {
					    	  templateFileNameText.setText(templateFileName);
							//getData();
						}
					}
				});
				
		//Input filename - Value line
		fileNameLabel=new Label(shell, SWT.RIGHT);
		fileNameLabel.setText(Messages.getString("IDXOutputPluginDialog.ValueToAdd.File.Label")); //$NON-NLS-1$
        props.setLook( fileNameLabel );
        fileForm4=new FormData();
        fileForm4.left = new FormAttachment(0, 0);
        fileForm4.right= new FormAttachment(middle, -margin);
        fileForm4.top  = new FormAttachment(templateFileNameText, margin);
        fileNameLabel.setLayoutData(fileForm4);

		fileButtonValue=new Button(shell, SWT.PUSH| SWT.CENTER);
        props.setLook( fileButtonValue );
        fileButtonValue.setText("Select File"); //$NON-NLS-1$
        fileForm5 = new FormData();
        fileForm5.right = new FormAttachment(100, 0);
        fileForm5.top = new FormAttachment(templateFileNameText, margin);
		fileButtonValue.setLayoutData(fileForm5);

		fileNameText = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(fileNameText);
        fileNameText.addModifyListener(lsMod);  
        fileForm6=new FormData();
        fileForm6.left = new FormAttachment(middle, 0);
        fileForm6.right= new FormAttachment(fileButtonValue, -margin);
        fileForm6.top  = new FormAttachment(templateFileNameText, margin);
        fileNameText.setLayoutData(fileForm6);

		fileButtonValue.addSelectionListener(new SelectionAdapter()
		{
			public void widgetSelected(SelectionEvent arg0)
			{
				  fileDialog=new FileDialog(shell,SWT.OPEN);
			      fileDialog.setText("Please enter output file Name:");
			      fileName=fileDialog.open();
			      if (fileName != null) {
			    	  fileNameText.setText(fileName);
					//getData();
				}
			}
		});

     // MatchIDField Line
     		wlMatchIDField=new Label(shell, SWT.RIGHT);
     		wlMatchIDField.setText(BaseMessages.getString(PKG, "IDXOutputPluginDialog.MatchIDField.Label")); //$NON-NLS-1$
      		props.setLook(wlMatchIDField);
     		fdlMatchIDField=new FormData();
     		fdlMatchIDField.left = new FormAttachment(0, 0);
     		fdlMatchIDField.right= new FormAttachment(middle, -margin);
     		fdlMatchIDField.top  = new FormAttachment(fileNameText, margin);
     		wlMatchIDField.setLayoutData(fdlMatchIDField);
     		
         	wMatchIDField=new ComboVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
     		props.setLook(wMatchIDField);
     		wMatchIDField.addModifyListener(lsMod);
     		fdMatchIDField=new FormData();
     		fdMatchIDField.left = new FormAttachment(middle, 0);
     		fdMatchIDField.top  = new FormAttachment(fileNameText, margin);
     		fdMatchIDField.right= new FormAttachment(100, 0);
     		wMatchIDField.setLayoutData(fdMatchIDField);
     		wMatchIDField.setEnabled(true);
     		wMatchIDField.addFocusListener(new FocusListener()
             {
                 public void focusLost(org.eclipse.swt.events.FocusEvent e)
                 {
                 }
             
                 public void focusGained(org.eclipse.swt.events.FocusEvent e)
                 {
                     Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
                     shell.setCursor(busy);
                     getFields();
                     shell.setCursor(null);
                     busy.dispose();
                 }
             }
         );  
     	// FileNameInField line
            wlFileNameInField=new Label(shell, SWT.RIGHT);
            wlFileNameInField.setText(BaseMessages.getString(PKG, "IDXOutputPluginDialog.FileNameInField.Label"));
            props.setLook(wlFileNameInField);
            fdlFileNameInField=new FormData();
            fdlFileNameInField.left = new FormAttachment(0, 0);
            fdlFileNameInField.top  = new FormAttachment(wMatchIDField, margin);
            fdlFileNameInField.right= new FormAttachment(middle, -margin);
            wlFileNameInField.setLayoutData(fdlFileNameInField);
            wFileNameInField=new Button(shell, SWT.CHECK );
            props.setLook(wFileNameInField);
            fdFileNameInField=new FormData();
            fdFileNameInField.left = new FormAttachment(middle, 0);
            fdFileNameInField.top  = new FormAttachment(wMatchIDField, margin);
            fdFileNameInField.right= new FormAttachment(100, 0);
            wFileNameInField.setLayoutData(fdFileNameInField);
            wFileNameInField.addSelectionListener(new SelectionAdapter() 
                {
                    public void widgetSelected(SelectionEvent e) 
                    {
                    	input.setChanged();
                    	activeFileNameField();        
                    }
                }
            );
            
         // FileNameField Line
         		wlFileNameField=new Label(shell, SWT.RIGHT);
         		wlFileNameField.setText(BaseMessages.getString(PKG, "IDXOutputPluginDialog.FileNameField.Label")); //$NON-NLS-1$
          		props.setLook(wlFileNameField);
         		fdlFileNameField=new FormData();
         		fdlFileNameField.left = new FormAttachment(0, 0);
         		fdlFileNameField.right= new FormAttachment(middle, -margin);
         		fdlFileNameField.top  = new FormAttachment(wFileNameInField, margin);
         		wlFileNameField.setLayoutData(fdlFileNameField);
         		
             	wFileNameField=new ComboVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
         		props.setLook(wFileNameField);
         		wFileNameField.addModifyListener(lsMod);
         		fdFileNameField=new FormData();
         		fdFileNameField.left = new FormAttachment(middle, 0);
         		fdFileNameField.top  = new FormAttachment(wFileNameInField, margin);
         		fdFileNameField.right= new FormAttachment(100, 0);
         		wFileNameField.setLayoutData(fdFileNameField);
         		wFileNameField.setEnabled(false);
         		wFileNameField.addFocusListener(new FocusListener()
                 {
                     public void focusLost(org.eclipse.swt.events.FocusEvent e)
                     {
                     }
                 
                     public void focusGained(org.eclipse.swt.events.FocusEvent e)
                     {
                         Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
                         shell.setCursor(busy);
                         getFields();
                         shell.setCursor(null);
                         busy.dispose();
                     }
                 }
             );  
		// File Format line
		fileFormatLabel=new Label(shell, SWT.RIGHT);
		fileFormatLabel.setText(Messages.getString("IDXOutputPluginDialog.ValueToAdd.FileFormat.Label")); //$NON-NLS-1$
        props.setLook( fileFormatLabel );
        fileForm7=new FormData();
        fileForm7.left = new FormAttachment(0, 0);
        fileForm7.right= new FormAttachment(middle, -margin);
        fileForm7.top  = new FormAttachment(wFileNameField, margin); //指到上一個text的位址，延續下去排列
		fileFormatLabel.setLayoutData(fileForm7);
		fileFormatText=new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
		fileFormatText.setText(Messages.getString("IDXOutputPluginDialog.ValueToAdd.FileFormat.Label")); //$NON-NLS-1$
        props.setLook( fileFormatText );
        String[] items = {"IDX", "GZIP"};
        fileFormatText.setItems(items);
        fileFormatText.addModifyListener(lsMod);
        fileForm8=new FormData();
        fileForm8.left = new FormAttachment(middle, 0);
        fileForm8.right= new FormAttachment(100, 0);
        fileForm8.top  = new FormAttachment(wFileNameField, margin);
		fileFormatText.setLayoutData(fileForm8);
		
	
		/*************************************************
        // KEY / LOOKUP TABLE
		/*************************************************/
		
        wlKeys=new Label(shell, SWT.NONE);
        wlKeys.setText("Fields:"); 
        props.setLook(wlKeys);
        FormData fdlReturn=new FormData();
        fdlReturn.left  = new FormAttachment(0, 0);
        fdlReturn.top   = new FormAttachment(fileFormatText, margin); //指到上一個text的位址，延續下去排列
        wlKeys.setLayoutData(fdlReturn);
        
        int keyWidgetCols=10;
        int keyWidgetRows= (input.getKeyField()!=null?input.getKeyField().length:1);
        
        ColumnInfo[] ciKeys=new ColumnInfo[keyWidgetCols];
        ciKeys[0]=new ColumnInfo(BaseMessages.getString(PKG, "IDXOutputPluginDialog.ColumnInfo.KeyField"),    ColumnInfo.COLUMN_TYPE_CCOMBO,  new String[]{}, false); 
        ciKeys[1]=new ColumnInfo(BaseMessages.getString(PKG, "IDXOutputPluginDialog.ColumnInfo.ValueField"),	ColumnInfo.COLUMN_TYPE_TEXT, false); 
        ciKeys[2]=new ColumnInfo(BaseMessages.getString(PKG, "IDXOutputPluginDialog.ColumnInfo.Default"),  	ColumnInfo.COLUMN_TYPE_TEXT,   false); 
        ciKeys[3]=new ColumnInfo(BaseMessages.getString(PKG, "IDXOutputPluginDialog.ColumnInfo.Type"),     	ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMeta.getTypes()); 
        ciKeys[4]=new ColumnInfo(BaseMessages.getString(PKG, "IDXOutputPluginDialog.ColumnInfo.Format"),    	ColumnInfo.COLUMN_TYPE_FORMAT, 4);
        ciKeys[5]=new ColumnInfo(BaseMessages.getString(PKG, "IDXOutputPluginDialog.ColumnInfo.Length"),    	ColumnInfo.COLUMN_TYPE_TEXT,   false);
        ciKeys[6]=new ColumnInfo(BaseMessages.getString(PKG, "IDXOutputPluginDialog.ColumnInfo.Precision"), 	ColumnInfo.COLUMN_TYPE_TEXT,   false);
        ciKeys[7]=new ColumnInfo(BaseMessages.getString(PKG, "IDXOutputPluginDialog.ColumnInfo.Currency"),  	ColumnInfo.COLUMN_TYPE_TEXT,   false);
        ciKeys[8]=new ColumnInfo(BaseMessages.getString(PKG, "IDXOutputPluginDialog.ColumnInfo.Decimal"),   	ColumnInfo.COLUMN_TYPE_TEXT,   false);
        ciKeys[9]=new ColumnInfo(BaseMessages.getString(PKG, "IDXOutputPluginDialog.ColumnInfo.Group"),     	ColumnInfo.COLUMN_TYPE_TEXT,   false);
         
        fieldColumn = ciKeys[0];
        
        wKeys=new TableView(transMeta, shell, 
                              SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, 
                              ciKeys, 
                              keyWidgetRows,  
                              lsMod,
                              props
                              );
        
        FormData fdReturn=new FormData();
        fdReturn.left  = new FormAttachment(0, 0);
        fdReturn.top   = new FormAttachment(wlKeys, margin);
        fdReturn.right = new FormAttachment(100, 0);
        fdReturn.bottom= new FormAttachment(100, -50);
        
        wKeys.setLayoutData(fdReturn);

       
		// Some buttons
		wOK=new Button(shell, SWT.PUSH);
		wOK.setText(Messages.getString("System.Button.OK")); //$NON-NLS-1$
		wGet = new Button(shell, SWT.PUSH);
		wGet.setText(Messages.getString("System.Button.Get"));
		wGet.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.GetFields"));
		
		wCancel=new Button(shell, SWT.PUSH);
		wCancel.setText(Messages.getString("System.Button.Cancel")); //$NON-NLS-1$

        BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wGet, wCancel}, margin, wValue);
        
        
		// Add listeners
		lsCancel   = new Listener() { public void handleEvent(Event e) { cancel(); } };
		lsGet  = new Listener() { public void handleEvent(Event e) { get();} };
		lsOK       = new Listener() { public void handleEvent(Event e) { ok();     } };
		
		wCancel.addListener(SWT.Selection, lsCancel);
		wGet.addListener(SWT.Selection, super.lsGet);
		wOK.addListener    (SWT.Selection, lsOK    );
		
		
		lsDef=new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };
		
		wStepname.addSelectionListener( lsDef );
		fileEncodingText.addSelectionListener( lsDef );
		fileNameText.addSelectionListener( lsDef );
		
		shell.addShellListener(new ShellAdapter() { public void shellClosed(ShellEvent e) { get(); } });
		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(	new ShellAdapter() { public void shellClosed(ShellEvent e) { cancel(); } } );
		

		// Set the shell size, based upon previous time...
		setSize();
		
		getData();
		activeFileNameField();
		//input.setChanged(changed);
		
		shell.open();
		while (!shell.isDisposed())
		{
		    if (!display.readAndDispatch()) display.sleep();
		}
		return stepname;
	}
	
	// Read data from input (TextFileInputInfo)
	public void getData()
	{
		wStepname.selectAll();
		if(input.getFileEncoding()!=null){
			fileEncodingText.setText(input.getFileEncoding());
		}
		if(input.getTemplateFileName()!=null){
			templateFileNameText.setText(input.getTemplateFileName());
			
		}
		if(input.getFileName()!=null){
			fileNameText.setText(input.getFileName());
			
		}
		if (input.getMatchIDField() !=null) wMatchIDField.setText(input.getMatchIDField());
		
		 wFileNameInField.setSelection(input.isFileNameInfield());
		 if (input.getFileNameField() !=null) wFileNameField.setText(input.getFileNameField());
		 

		if(input.getFileFormat()!=null){
			fileFormatText.setText(input.getFileFormat());
			
		}
		
		if (input.getKeyField()!=null){
			
			
			for (int i=0;i<input.getKeyField().length;i++){
				
				TableItem item = wKeys.table.getItem(i);
				
				if (input.getKeyField()[i] != null){
					item.setText(1, input.getKeyField()[i]);
				} 
				
				if (input.getOutputField()[i] != null){
					item.setText(2, input.getOutputField()[i]);
				} 
				
				if (input.getOutputDefault()[i] != null){
					item.setText(3, input.getOutputDefault()[i]);	
				}
				
				item.setText(4, ValueMeta.getTypeDesc(input.getOutputType()[i]));
				
				if (input.getOutputFormat()[i] != null){
					item.setText(5, input.getOutputFormat()[i]);	
				}
				item.setText(6, input.getOutputLength()[i]<0?"":""+input.getOutputLength()[i]);
				item.setText(7, input.getOutputPrecision()[i]<0?"":""+input.getOutputPrecision()[i]);
				
				if (input.getOutputCurrency()[i] != null){
					item.setText(8, input.getOutputCurrency()[i]);	
				}
				
				if (input.getOutputDecimal()[i] != null){
					item.setText(9, input.getOutputDecimal()[i]);
				} 
				
				if (input.getOutputGroup()[i] != null){
					item.setText(10, input.getOutputGroup()[i]);
				} 
				
			}
		}
		
		/*
		if(input.getKeyField()!=null){
			int num_items = input.getKeyField().length;
			for(int i=0;i<num_items;i++){
				TableItem item1=table.getItem(i);
				item1.setText(new String[]{Integer.toString(i),input.getKeyField()[i],input.getOutputField()[i],input.getOutputDefault()[i]});
			}
		}*/
		
        wKeys.removeEmptyRows();
        //wKeys.setRowNums();
		wKeys.optWidth(true);	
        /*
        wKeys.table.getColumn(1).setWidth(150);
        wKeys.table.getColumn(1).setAlignment(SWT.CENTER);
        wKeys.table.getColumn(2).setWidth(150);
        wKeys.table.getColumn(2).setAlignment(SWT.CENTER);
        wKeys.table.getColumn(3).setWidth(150);
        wKeys.table.getColumn(3).setAlignment(SWT.CENTER);	
        wKeys.table.getColumn(4).setWidth(400);
        wKeys.table.getColumn(4).setAlignment(SWT.CENTER);
        */
		
		
	}
	
	// asynchronous filling of the combo boxes
	
		  
	private void cancel(){
		stepname=null;
		input.setChanged(changed);
		dispose();
	}
	
	
	private void get()
	{
		try
		{
			RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			if (r!=null)
			{
                TableItemInsertListener listener = new TableItemInsertListener()
                    {
                        public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v)
                        {
                            if (v.isNumber())
                            {
                                if (v.getLength()>0)
                                {
                                    int le=v.getLength();
                                    int pr=v.getPrecision();
                                    
                                    if (v.getPrecision()<=0)
                                    {
                                        pr=0;
                                    }
                                    
                                    String mask="";
                                    for (int m=0;m<le-pr;m++)
                                    {
                                        mask+="0";
                                    }
                                    if (pr>0) mask+=".";
                                    for (int m=0;m<pr;m++)
                                    {
                                        mask+="0";
                                    }
                                    tableItem.setText(3, mask);
                                }
                            }
                            return true;
                        }
                    };
                BaseStepDialog.getFieldsFromPrevious(r, wKeys, 0, new int[] { 2 }, new int[] { 4 }, 6, 7, listener);
			}
		} catch(KettleException ke)	{
			
			new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"), BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"), ke);
		
		}

	}
	
		
	private void ok()
	{
		//Display display = shell.getDisplay();
		stepname = wStepname.getText(); // return value
		//value.getValueMeta().setName(wValName.getText());
		//input.setValue(value);
		input.setFileEncoding(fileEncodingText.getText());
		input.setTemplateFileName(templateFileNameText.getText());
		input.setFileName(fileNameText.getText());
		input.setMatchIDField(wMatchIDField.getText());
		input.setFileNameField(wFileNameField.getText());
		input.setFileNameInfield(wFileNameInField.getSelection());
		input.setFileFormat(fileFormatText.getText());


		int nrKeys= wKeys.nrNonEmpty();
		
		input.allocate(nrKeys);
		
		for (int i=0;i<nrKeys;i++)
		{
			TableItem item = wKeys.getNonEmpty(i);
			input.getKeyField()[i] = item.getText(1);
			input.getOutputField()[i] = item.getText(2);
			
			input.getOutputDefault()[i] = item.getText(3);
			input.getOutputType()[i] = ValueMeta.getType(item.getText(4));

			// fix unknowns
			if (input.getOutputType()[i]<0){
				input.getOutputType()[i]=ValueMetaInterface.TYPE_STRING;
			}
			
			input.getOutputFormat()[i] = item.getText(5); 
			input.getOutputLength()[i] = Const.toInt(item.getText(6), -1);
			input.getOutputPrecision()[i] = Const.toInt(item.getText(7), -1);
			input.getOutputCurrency()[i] = item.getText(8);
			input.getOutputDecimal()[i] = item.getText(9);
			input.getOutputGroup()[i] = item.getText(10);
			
		}		
		dispose();
	}
	
    private void getFields()
	 { //匯入欄位ID到MatchID和FileName 
		if(!gotPreviousFields)
		{
		 try{
			 String field=wFileNameField.getText();
			 String field2=wMatchIDField.getText();
			 RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			 if(r!=null)
			  {
				 wFileNameField.setItems(r.getFieldNames());
				 wMatchIDField.setItems(r.getFieldNames());
			  }
			 if(field!=null) wFileNameField.setText(field);
			 if(field2!=null) wMatchIDField.setText(field2);
		 	}catch(KettleException ke){
				new ErrorDialog(shell, BaseMessages.getString(PKG, "TextFileOutputDialog.FailedToGetFields.DialogTitle"), BaseMessages.getString(PKG, "TextFileOutputDialog.FailedToGetFields.DialogMessage"), ke);
			}
		 	gotPreviousFields=true;
		}
	 }
    
	private void activeFileNameField()
	{
	   	wlFileNameField.setEnabled(wFileNameInField.getSelection());
	   	wFileNameField.setEnabled(wFileNameInField.getSelection());
    	//wlExtension.setEnabled(!wFileNameInField.getSelection());
	   	
	   	fileNameLabel.setEnabled(!wFileNameInField.getSelection());
	   	fileNameText.setEnabled(!wFileNameInField.getSelection());
    	
    	if(wFileNameInField.getSelection()) 
    	{
    		/*
    		if(!wDoNotOpenNewFileInit.getSelection())
    			wDoNotOpenNewFileInit.setSelection(true);
    		
    		wAddDate.setSelection(false);
    		wAddTime.setSelection(false);
    		wSpecifyFormat.setSelection(false);
    		wAddStepnr.setSelection(false);
    		wAddPartnr.setSelection(false);
    		*/
    	}
    	
    	//wlDoNotOpenNewFileInit.setEnabled(!wFileNameInField.getSelection());
    	//wDoNotOpenNewFileInit.setEnabled(!wFileNameInField.getSelection());
    	/*
    	wlSpecifyFormat.setEnabled(!wFileNameInField.getSelection());
    	wSpecifyFormat.setEnabled(!wFileNameInField.getSelection());
    	
    	wAddStepnr.setEnabled(!wFileNameInField.getSelection());
    	wlAddStepnr.setEnabled(!wFileNameInField.getSelection());
    	wAddPartnr.setEnabled(!wFileNameInField.getSelection());
    	wlAddPartnr.setEnabled(!wFileNameInField.getSelection());
    	if (wFileNameInField.getSelection()) wSplitEvery.setText("0");
    	wSplitEvery.setEnabled(!wFileNameInField.getSelection());
    	wlSplitEvery.setEnabled(!wFileNameInField.getSelection());
    	if (wFileNameInField.getSelection()) wEndedLine.setText("");    	
      	wEndedLine.setEnabled(!wFileNameInField.getSelection());
      	wbShowFiles.setEnabled(!wFileNameInField.getSelection());
      	wbFilename.setEnabled(!wFileNameInField.getSelection());
      	*/
      	//setDateTimeFormat();
    }
}
