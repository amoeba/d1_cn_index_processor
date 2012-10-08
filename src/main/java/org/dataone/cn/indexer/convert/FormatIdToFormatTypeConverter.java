package org.dataone.cn.indexer.convert;

import org.dataone.client.ObjectFormatCache;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;

public class FormatIdToFormatTypeConverter implements IConverter {

    @Override
    public String convert(String formatId) {
        String formatType = formatId;
        if (formatId != null) {
            ObjectFormat format = null;
            try {
                ObjectFormatIdentifier objectFormat = new ObjectFormatIdentifier();
                objectFormat.setValue(formatId);
                format = ObjectFormatCache.getInstance().getFormat(objectFormat);
                formatType = format.getFormatType();
            } catch (BaseException e) {
                return "";
            }
        }
        return formatType;
    }

}
