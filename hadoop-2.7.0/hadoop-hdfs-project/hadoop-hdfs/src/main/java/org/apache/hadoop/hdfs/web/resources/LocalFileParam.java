package org.apache.hadoop.hdfs.web.resources;
/*
 * added by nicolas
 * Parameter to be used for specifying the local path of a file that
 * it is about to be created
 */
public class LocalFileParam extends StringParam{
	/** Parameter name. */
	public static final String NAME = "local";
	/** Default parameter value. */
	public static final String DEFAULT = "";
	
	private static final Domain DOMAIN = new Domain(NAME, null);
	
	/**
	   * Constructor.
	   * @param str a string representation of the parameter value.
	   */
	public LocalFileParam(final String str) {
		super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
	}
	
	@Override
	  public String getName() {
	    return NAME;
	  }
}
