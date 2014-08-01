package au.gov.ga.conn4DUtils;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Performs basic time conversions to and from milliseconds.  This is
 * intended for *simple conversions ONLY*, which is why months, dates etc.
 * are not included, nor are time zone shifts, daylight savings time etc.
 * accounted for.  For those types of conversions, use JODA, or the upcoming
 * Time and Date API in version 1.8.
 * 
 * @author Johnathan Kool
 */

public class TimeConvert {

	public static final long SECS_IN_DAY = 24 * 60 * 60;
	public static final long MILLIS_IN_DAY = 24 * 60 * 60 * 1000;
	public static final long HYCOM_OFFSET = -2177539200000l;

	public static double convertFromMillis(String unit, double millis) {
		if (unit.equalsIgnoreCase("Days") || unit.equalsIgnoreCase("Day")
				|| unit.equalsIgnoreCase("D")) {
			return millisToDays(millis);
		} else if (unit.equalsIgnoreCase("Seconds")
				|| unit.equalsIgnoreCase("Second")
				|| unit.equalsIgnoreCase("Secs")
				|| unit.equalsIgnoreCase("Sec") || unit.equalsIgnoreCase("S")) {
			return millisToSeconds(millis);
		} else if (unit.equalsIgnoreCase("Hours")
				|| unit.equalsIgnoreCase("Hour")
				|| unit.equalsIgnoreCase("Hrs") || unit.equalsIgnoreCase("Hr")
				|| unit.equalsIgnoreCase("H")) {
			return millisToHours(millis);
		} else if (unit.equalsIgnoreCase("Minutes")
				|| unit.equalsIgnoreCase("Minute")
				|| unit.equalsIgnoreCase("Mins")
				|| unit.equalsIgnoreCase("Min") || unit.equalsIgnoreCase("M")) {
			return millisToMinutes(millis);
		}
		else
			throw new UnsupportedOperationException("Conversion of unit "
					+ unit + "has not been implemented");
	}

	/**
	 * Converts time measurements to milliseconds.
	 * 
	 * @param unit
	 *            - Original units of time
	 * @param val
	 *            - Number of units (as a double)
	 */

	public static long convertToMillis(String unit, double val) {

		if (unit.equalsIgnoreCase("Days") || unit.equalsIgnoreCase("Day")
				|| unit.equalsIgnoreCase("D")) {
			return daysToMillis(val);
		} else if (unit.equalsIgnoreCase("Seconds")
				|| unit.equalsIgnoreCase("Second")
				|| unit.equalsIgnoreCase("Secs")
				|| unit.equalsIgnoreCase("Sec") || unit.equalsIgnoreCase("S")) {
			return secondsToMillis(val);
		} else if (unit.equalsIgnoreCase("Hours")
				|| unit.equalsIgnoreCase("Hour")
				|| unit.equalsIgnoreCase("Hrs") || unit.equalsIgnoreCase("Hr")
				|| unit.equalsIgnoreCase("H")) {
			return hoursToMillis(val);
		} else if (unit.equalsIgnoreCase("Minutes")
				|| unit.equalsIgnoreCase("Minute")
				|| unit.equalsIgnoreCase("Mins")
				|| unit.equalsIgnoreCase("Min") || unit.equalsIgnoreCase("M")) {
			return minutesToMillis(val);
		} else if (unit.equalsIgnoreCase("Milliseconds")
				|| unit.equalsIgnoreCase("Millisecond")
				|| unit.equalsIgnoreCase("ms")) {
			return (long) val;
		} else
			throw new UnsupportedOperationException("Conversion of unit "
					+ unit + "has not been implemented");
	}

	/**
	 * Converts time measurements into milliseconds.
	 * 
	 * @param unit
	 *            - Original units of time
	 * @param val
	 *            - Number of units
	 */

	public static long convertToMillis(String unit, String val) {

		if (unit.equalsIgnoreCase("Days") || unit.equalsIgnoreCase("Day")
				|| unit.equalsIgnoreCase("D")) {
			return daysToMillis(val);
		} else if (unit.equalsIgnoreCase("Seconds")
				|| unit.equalsIgnoreCase("Second")
				|| unit.equalsIgnoreCase("Secs")
				|| unit.equalsIgnoreCase("Sec") || unit.equalsIgnoreCase("S")) {
			return secondsToMillis(val);
		} else if (unit.equalsIgnoreCase("Date")) {
			try {
				DateFormat df = new SimpleDateFormat("M/d/yyyy");
				Date d = df.parse(val);
				return d.getTime();
			} catch (ParseException e) {
				throw new IllegalArgumentException("Date could not be parsed: "
						+ val + ".  Required format is M/d/yyyy.");
			}
		} else if (unit.equalsIgnoreCase("Hours")
				|| unit.equalsIgnoreCase("Hour")
				|| unit.equalsIgnoreCase("Hrs") || unit.equalsIgnoreCase("Hr")
				|| unit.equalsIgnoreCase("H")) {
			return hoursToMillis(val);
		} else if (unit.equalsIgnoreCase("Minutes")
				|| unit.equalsIgnoreCase("Minute")
				|| unit.equalsIgnoreCase("Mins")
				|| unit.equalsIgnoreCase("Min") || unit.equalsIgnoreCase("M")) {
			return minutesToMillis(val);
		}

		else
			throw new UnsupportedOperationException("Conversion of unit "
					+ unit + "has not been implemented");

	}

	/**
	 * Convert a date into milliseconds using the specified format.
	 * 
	 * @param date
	 *            - String representation of the date to be converted.
	 */

	public static long dateToMillis(String date) {
		DateFormat df = new SimpleDateFormat("M/d/yyyy");
		Date d;
		try {
			d = df.parse(date);
			return d.getTime();
		} catch (ParseException e) {
			System.out.println("WARNING:  Date provided: " + date
					+ "could not be parsed.\n\n");
			e.printStackTrace();
		}
		return -1;
	}

	/**
	 * Converts days into milliseconds
	 * 
	 * @param days
	 *            - String representation of the number of days to be converted.
	 */

	public static long daysToMillis(double days) {
		return Math.round(days * 24 * 60 * 60 * 1000);
	}

	/**
	 * Converts days into milliseconds
	 * 
	 * @param days
	 *            - Number of days
	 */

	public static long daysToMillis(String days) {
		double d = Double.parseDouble(days);
		return Math.round(d * 24 * 60 * 60 * 1000);
	}

	/**
	 * Converts from hours to milliseconds
	 * 
	 * @param hours
	 */

	public static long hoursToMillis(double hours) {
		return Math.round(hours * 60 * 60 * 1000);
	}

	/**
	 * Converts hours into milliseconds
	 * 
	 * @param hours
	 *            - String representation of the number of hours to be
	 *            converted.
	 */

	public static long hoursToMillis(String hours) {
		double d = Double.parseDouble(hours);
		return Math.round(d * 60 * 60 * 1000);
	}
	
	/**
	 * Converts HYCOM time (from 1900) to a Java Date Object.
	 * 
	 * @param days - the number of days since Jan 1 1900.
	 */

	public static Date HYCOMToJavaDate(long days) {
		return new Date(daysToMillis(days) + HYCOM_OFFSET);
	}

	/**
	 * Converts HYCOM time (from 1900) to Java milliseconds.
	 * 
	 * @param days - the number of days since Jan 1 1900.
	 */
	
	public static long HYCOMToJavaLong(long days) {
		return days + (long) millisToDays(-HYCOM_OFFSET);
	}

	/**
	 * Converts a HYCOM time to Java milliseconds
	 * 
	 * @param days - the number of days since Jan 1 1900.
	 */

	public static long HYCOMToMillis(long days) {
		return daysToMillis(days) + HYCOM_OFFSET;
	}

	/**
	 * Converts a Java Date to a HYCOM time
	 */

	public static long javaDateToHYCOM(Date d) {
		return (long) millisToDays(d.getTime() - HYCOM_OFFSET);
	}

	/**
	 * Converts a Java long value (days) to a HYCOM time
	 */

	public static long javaLongToHYCOM(long days) {
		return days - (long) millisToDays(-HYCOM_OFFSET);
	}

	/**
	 * Converts milliseconds into a Java Date.  Useful for
	 * classes that import TimeConvert but not Date.
	 * 
	 * @param millis
	 */
	
	public static Date millisToDate(double millis) {
		return new Date((long) millis);
	}

	/**
	 * Converts milliseconds to days.
	 * 
	 * @param millis - milliseconds as a double value.
	 */
	
	public static double millisToDays(double millis) {
		return (millis) / (24f * 60f * 60f * 1000f);
	}

	/**
	 * Converts milliseconds to days
	 * 
	 * @param millis - milliseconds as a long value.
	 */
	
	public static double millisToDays(long millis) {
		return ((double) millis) / (24f * 60f * 60f * 1000f);
	}

	/**
	 * Converts milliseconds to hours
	 * 
	 * @param millis - milliseconds as a long value.
	 */
	
	public static double millisToHours(double millis) {
		return millis / (60f * 60f * 1000f);
	}

	/**
	 * Converts Java milliseconds to a HYCOM time
	 */

	public static double millisToHYCOM(double millis) {
		return millisToDays(millis - HYCOM_OFFSET);
	}

	/**
	 * Converts milliseconds to minutes
	 * 
	 * @param millis - milliseconds as a long value.
	 */
	
	public static double millisToMinutes(double millis) {
		return millis / (60f * 1000f);
	}
	
	/**
	 * Converts milliseconds to seconds
	 * 
	 * @param millis - milliseconds as a long value.
	 */

	public static double millisToSeconds(double millis) {
		return millis / 1000f;
	}

	/**
	 * Converts from milliseconds to a simple String representation.
	 * 
	 * @param millis
	 */

	public static String millisToString(double millis) {
		double remainder = millis;
		double day = 1000 * 60 * 60 * 24;
		int days = (int) Math.floor(millis / day);
		remainder = millis - (days * day);
		double hour = 1000 * 60 * 60;
		int hours = (int) Math.floor(remainder / hour);
		remainder = remainder - (hours * hour);
		double minute = 1000 * 60;
		int minutes = (int) Math.floor(remainder / minute);
		remainder = remainder - (minute * minutes);
		double seconds = remainder / 1000;
		
		DecimalFormat df = new DecimalFormat("#0.000");

		StringBuffer sb = new StringBuffer();

		if (days != 0) {
			sb.append(days + "d, ");
		}
		if (hours != 0 || days != 0) {
			sb.append(hours + "h ");
		}
		if (minutes != 0 || days != 0 || hours != 0) {
			sb.append(minutes + "m ");
		}
		sb.append(df.format(seconds) + "s");

		return sb.toString();
	}

	/**
	 * Converts from minutes to milliseconds
	 * 
	 * @param minutes
	 */

	public static long minutesToMillis(double minutes) {
		return Math.round(minutes * 60 * 1000);
	}

	/**
	 * Converts minutes into milliseconds
	 * 
	 * @param minutes
	 *            - String representation of the number of minutes to be
	 *            converted.
	 */

	public static long minutesToMillis(String minutes) {
		double d = Double.parseDouble(minutes);
		return Math.round(d * 60 * 1000);
	}

	/**
	 * Converts from seconds to milliseconds
	 * 
	 * @param seconds
	 */

	public static long secondsToMillis(double seconds) {
		return Math.round(seconds * 1000);
	}

	/**
	 * Convenience function for Converting seconds into milliseconds (multiply
	 * by 1000).
	 * 
	 * @param seconds
	 */

	public static long secondsToMillis(String seconds) {
		double d = Double.parseDouble(seconds);
		return Math.round(d * 1000);
	}
}
