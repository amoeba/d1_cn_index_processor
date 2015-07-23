package org.dataone.cn.indexer.parser.utility;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;

public class SeriesIdResolver {
	
    public static Log log = LogFactory.getLog(SeriesIdResolver.class);

    /**
     * Method to find HEAD PID for a given SID.
     * If the provided identifier is already a PID, then it will simply be returned.
     * If the provided identifier is a SID, then the latest SystemMetadata will be fetched
     * and the PID for this latest revision will be returned.
     * @param identifier the SID to look up (if a PID is provided it will simply be returned)
     * @return the HEAD PID for the given identifier
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotAuthorized
     * @throws NotFound
     * @throws NotImplemented
     */
	public static Identifier getPid(Identifier identifier) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
		// check if this is this a sid
		Identifier pid = identifier;
        log.debug("pid===" + pid.getValue());
        SystemMetadata fetchedSysmeta = D1Client.getCN().getSystemMetadata(null, identifier);
        if (!fetchedSysmeta.getIdentifier().getValue().equals(identifier.getValue())) {
            log.debug("Found pid: " + fetchedSysmeta.getIdentifier().getValue() + " for sid: " + identifier.getValue());
            pid = fetchedSysmeta.getIdentifier();
        }
        
        return pid;
	}
	
	/**
	 * Check if the given identifier is a PID or a SID
	 * @param identifier
	 * @return true if the identifier is a SID, false if a PID
	 */
	public static boolean isSeriesId(Identifier identifier) {
		
		// if we have system metadata available via HZ map, then it's a PID
		SystemMetadata systemMetadata = HazelcastClientFactory.getSystemMetadataMap().get(identifier);
		if (systemMetadata != null) {
			return false;
		}
		
		//TODO: check that it's not just bogus value by looking up the pid?
//		Identifier pid = getPid(identifier);
//		if (pid.equals(identifier)) {
//			return false;
//		}
		
		// okay, it's a SID
		return true;
		
	}
	
}
