package org.knime.core.table.virtual.graph.util;

import org.knime.core.table.access.BooleanAccess;
import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess;

public class ReadAccessUtils {
    public  static String toString(final ReadAccess access) {
        if (access instanceof IntAccess.IntReadAccess) {
            final var a = (IntAccess.IntReadAccess)access;
            return "(int) " + (a.isMissing() ? "-missing-" : a.getIntValue());
        } else if (access instanceof StringAccess.StringReadAccess) {
            final var a = (StringAccess.StringReadAccess)access;
            return "(string) " + (a.isMissing() ? "-missing-" : a.getStringValue());
        } else if (access instanceof DoubleAccess.DoubleReadAccess) {
            final var a = (DoubleAccess.DoubleReadAccess)access;
            return "(double) " + (a.isMissing() ? "-missing-" : a.getDoubleValue());
        } else if (access instanceof BooleanAccess.BooleanReadAccess) {
            final var a = (BooleanAccess.BooleanReadAccess)access;
            return "(boolean) " + (a.isMissing() ? "-missing-" : a.getBooleanValue());
        }
        throw new UnsupportedOperationException("not implemented yet");
    }
}
