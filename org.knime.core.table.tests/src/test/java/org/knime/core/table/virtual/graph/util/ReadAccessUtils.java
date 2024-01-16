package org.knime.core.table.virtual.graph.util;

import org.knime.core.table.access.BooleanAccess;
import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.access.LongAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess;

public class ReadAccessUtils {
    public  static String toString(final ReadAccess access) {
        if (access == null) {
            return "null";
        } else if (access instanceof IntAccess.IntReadAccess) {
            final IntAccess.IntReadAccess a = (IntAccess.IntReadAccess)access;
            return "(int) " + (a.isMissing() ? "-missing-" : a.getIntValue());
        } else if (access instanceof LongAccess.LongReadAccess) {
            final LongAccess.LongReadAccess a = (LongAccess.LongReadAccess)access;
            return "(long) " + (a.isMissing() ? "-missing-" : a.getLongValue());
        } else if (access instanceof StringAccess.StringReadAccess) {
            final StringAccess.StringReadAccess a = (StringAccess.StringReadAccess)access;
            return "(string) " + (a.isMissing() ? "-missing-" : a.getStringValue());
        } else if (access instanceof DoubleAccess.DoubleReadAccess) {
            final DoubleAccess.DoubleReadAccess a = (DoubleAccess.DoubleReadAccess)access;
            return "(double) " + (a.isMissing() ? "-missing-" : a.getDoubleValue());
        } else if (access instanceof BooleanAccess.BooleanReadAccess) {
            final BooleanAccess.BooleanReadAccess a = (BooleanAccess.BooleanReadAccess)access;
            return "(bool) " + (a.isMissing() ? "-missing-" : a.getBooleanValue());
        }
        throw new UnsupportedOperationException("not implemented yet (access = " + access + ")");
    }
}
