package org.thingsboard.server.transport.lwm2m.server.client;

import org.eclipse.leshan.server.registration.Registration;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.util.Map;
import java.util.UUID;

public interface LwM2mClientContext {

    void delRemoveSessionAndListener(String registrationId);

    LwM2mClient getLwM2MClient(String endPoint, String identity);

    LwM2mClient getLwM2MClient(TransportProtos.SessionInfoProto sessionInfo);

    LwM2mClient getLwM2mClient(UUID sessionId);

    LwM2mClient getLwM2mClientWithReg(Registration registration, String registrationId);

    LwM2mClient updateInSessionsLwM2MClient(Registration registration);

    Registration getRegistration(String registrationId);

    Map<String, LwM2mClient> getLwM2mClients();

    Map<UUID, LwM2mClientProfile> getProfiles();

    LwM2mClientProfile getProfile(UUID profileUuId);

    LwM2mClientProfile getProfile(String registrationId);

    Map<UUID, LwM2mClientProfile> setProfiles(Map<UUID, LwM2mClientProfile> profiles);

    boolean addUpdateProfileParameters(DeviceProfile deviceProfile);
}
