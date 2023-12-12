package org.apache.hadoop.hdfs.web.resources;

public class StringReplicationParam extends StringParam{
	/** Parameter name. */
	public static final String NAME = "vector";
	/** Default parameter value. */
	public static final String DEFAULT = "";
	
	private static final Domain DOMAIN = new Domain(NAME, null);
	
	/**
	   * Constructor.
	   * @param str a string representation of the parameter value.
	   */
	public StringReplicationParam(final String str) {
		super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
	}
	
	@Override
	  public String getName() {
	    return NAME;
	  }
}
