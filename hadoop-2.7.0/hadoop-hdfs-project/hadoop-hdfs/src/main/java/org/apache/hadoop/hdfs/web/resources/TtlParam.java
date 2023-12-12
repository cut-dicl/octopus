package org.apache.hadoop.hdfs.web.resources;

/*
 * added by nicolas
 * Class to handle storage (type) parameter
 */
public class TtlParam extends StringParam{
	/** Parameter name. */
	public static final String NAME = "maxttl";
	/** Default parameter value. */
	public static final String DEFAULT = "";
	
	private static final Domain DOMAIN = new Domain(NAME, null);
	
	/**
	   * Constructor.
	   * @param str a string representation of the parameter value.
	   */
	public TtlParam(final String str) {
		super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
	}
	
	@Override
	  public String getName() {
	    return NAME;
	  }
}
