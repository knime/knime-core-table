package org.knime.core.table.virtual.graph.util;

import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess;

public class ReadAccessUtils {
    public  static String toString(final ReadAccess access) {
        if (access instanceof IntAccess.IntReadAccess) {
            final IntAccess.IntReadAccess a = (IntAccess.IntReadAccess)access;
            return "(int) " + (a.isMissing() ? "-missing-" : a.getIntValue());
        } else if (access instanceof StringAccess.StringReadAccess) {
            final StringAccess.StringReadAccess a = (StringAccess.StringReadAccess)access;
            return "(string) " + (a.isMissing() ? "-missing-" : a.getStringValue());
        } else if (access instanceof DoubleAccess.DoubleReadAccess) {
            final DoubleAccess.DoubleReadAccess a = (DoubleAccess.DoubleReadAccess)access;
            return "(double) " + (a.isMissing() ? "-missing-" : a.getDoubleValue());
        }
        throw new UnsupportedOperationException("not implemented yet");
    }
}
