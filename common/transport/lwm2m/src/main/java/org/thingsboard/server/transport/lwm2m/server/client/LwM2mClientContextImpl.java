package org.thingsboard.server.transport.lwm2m.server.client;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.leshan.core.util.Hex;
import org.eclipse.leshan.server.registration.Registration;
import org.eclipse.leshan.server.security.EditableSecurityStore;
import org.eclipse.leshan.server.security.SecurityInfo;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.util.TbLwM2mTransportComponent;
import org.thingsboard.server.transport.lwm2m.secure.LwM2MSecurityMode;
import org.thingsboard.server.transport.lwm2m.secure.LwM2mCredentialsSecurityInfoValidator;
import org.thingsboard.server.transport.lwm2m.secure.ReadResultSecurityStore;
import org.thingsboard.server.transport.lwm2m.server.LwM2mTransportHandler;
import org.thingsboard.server.transport.lwm2m.utils.TypeServer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.thingsboard.server.transport.lwm2m.secure.LwM2MSecurityMode.NO_SEC;

@Service
@TbLwM2mTransportComponent
@Slf4j
public class LwM2mClientContextImpl implements LwM2mClientContext {
    private static final boolean INFOS_ARE_COMPROMISED = false;

    private final Map<String /** registrationId */, LwM2mClient> lwM2mClients = new ConcurrentHashMap<>();
    private Map<UUID /** profileUUid */, LwM2mClientProfile> profiles = new ConcurrentHashMap<>();

    private final LwM2mCredentialsSecurityInfoValidator lwM2MCredentialsSecurityInfoValidator;

    private final EditableSecurityStore securityStore;

    public LwM2mClientContextImpl(LwM2mCredentialsSecurityInfoValidator lwM2MCredentialsSecurityInfoValidator, EditableSecurityStore securityStore) {
        this.lwM2MCredentialsSecurityInfoValidator = lwM2MCredentialsSecurityInfoValidator;
        this.securityStore = securityStore;
    }

    public void delRemoveSessionAndListener(String registrationId) {
        LwM2mClient lwM2MClient = lwM2mClients.get(registrationId);
        if (lwM2MClient != null) {
            securityStore.remove(lwM2MClient.getEndpoint(), INFOS_ARE_COMPROMISED);
            lwM2mClients.remove(registrationId);
        }
    }

    public LwM2mClient getLwM2MClient(String endPoint, String identity) {
        Map.Entry<String, LwM2mClient> modelClients = endPoint != null ?
                this.lwM2mClients.entrySet().stream().filter(model -> endPoint.equals(model.getValue().getEndpoint())).findAny().orElse(null) :
                this.lwM2mClients.entrySet().stream().filter(model -> identity.equals(model.getValue().getIdentity())).findAny().orElse(null);
        return modelClients != null ? modelClients.getValue() : null;
    }

    public LwM2mClient getLwM2MClient(TransportProtos.SessionInfoProto sessionInfo) {
        return getLwM2mClient(new UUID(sessionInfo.getSessionIdMSB(), sessionInfo.getSessionIdLSB()));
    }

    public LwM2mClient getLwM2mClient(UUID sessionId) {
        return lwM2mClients.values().stream().filter(c -> c.getSessionId().equals(sessionId)).findAny().get();
    }

    public LwM2mClient getLwM2mClientWithReg(Registration registration, String registrationId) {
        return registrationId != null ?
                this.lwM2mClients.get(registrationId) :
                this.lwM2mClients.containsKey(registration.getId()) ?
                        this.lwM2mClients.get(registration.getId()) :
                        this.lwM2mClients.get(registration.getEndpoint());
    }

    /**
     * Update in sessions (LwM2MClient for key registration_Id) after starting registration LwM2MClient in LwM2MTransportServiceImpl
     * Remove from sessions LwM2MClient with key registration_Endpoint
     * @param registration -
     * @return LwM2MClient after adding it to session
     */
    public LwM2mClient updateInSessionsLwM2MClient(Registration registration) {
            if (this.lwM2mClients.get(registration.getEndpoint()) == null) {
                addLwM2mClientToSession(registration.getEndpoint());
            }
            LwM2mClient lwM2MClient = lwM2mClients.get(registration.getEndpoint());
            lwM2MClient.setRegistration(registration);
            this.lwM2mClients.remove(registration.getEndpoint());
            this.lwM2mClients.put(registration.getId(), lwM2MClient);
            return lwM2MClient;
    }

    public Registration getRegistration(String registrationId) {
        return this.lwM2mClients.get(registrationId).getRegistration();
    }

    /**
     * Add new LwM2MClient to session
     * @param identity-
     * @return SecurityInfo. If error - SecurityInfoError
     * and log:
     * - FORBIDDEN - if there is no authorization
     * - profileUuid - if the device does not have a profile
     * - device - if the thingsboard does not have a device with a name equal to the identity
     */
    private SecurityInfo addLwM2mClientToSession(String identity) {
        ReadResultSecurityStore store = lwM2MCredentialsSecurityInfoValidator.createAndValidateCredentialsSecurityInfo(identity, TypeServer.CLIENT);
        if (store.getSecurityMode() < LwM2MSecurityMode.DEFAULT_MODE.code) {
            UUID profileUuid = (store.getDeviceProfile() != null && addUpdateProfileParameters(store.getDeviceProfile())) ? store.getDeviceProfile().getUuidId() : null;
            if (store.getSecurityInfo() != null && profileUuid != null) {
                String endpoint = store.getSecurityInfo().getEndpoint();
                lwM2mClients.put(endpoint, new LwM2mClient(endpoint, store.getSecurityInfo().getIdentity(), store.getSecurityInfo(), store.getMsg(), profileUuid, UUID.randomUUID()));
            } else if (store.getSecurityMode() == NO_SEC.code && profileUuid != null) {
                lwM2mClients.put(identity, new LwM2mClient(identity, null, null, store.getMsg(), profileUuid, UUID.randomUUID()));
            } else {
                log.error("Registration failed: FORBIDDEN/profileUuid/device [{}] , endpointId: [{}]", profileUuid, identity);
                /**
                 * Return Error securityInfo
                 */
                byte[] preSharedKey = Hex.decodeHex("0A0B".toCharArray());
                SecurityInfo infoError = SecurityInfo.newPreSharedKeyInfo("error", "error_identity", preSharedKey);
                return infoError;
            }
        }
        return store.getSecurityInfo();
    }

    public Map<String, LwM2mClient> getLwM2mClients() {
        return lwM2mClients;
    }

    public Map<UUID, LwM2mClientProfile> getProfiles() {
        return profiles;
    }

    public LwM2mClientProfile getProfile(UUID profileId) {
        return profiles.get(profileId);
    }

    public LwM2mClientProfile getProfile(String registrationId) {
        UUID profileId = this.getLwM2mClients().get(registrationId).getProfileId();
        return this.getProfiles().get(profileId);
    }

    public Map<UUID, LwM2mClientProfile> setProfiles(Map<UUID, LwM2mClientProfile> profiles) {
        return this.profiles = profiles;
    }

    public boolean addUpdateProfileParameters(DeviceProfile deviceProfile) {
        LwM2mClientProfile lwM2MClientProfile = LwM2mTransportHandler.getLwM2MClientProfileFromThingsboard(deviceProfile);
        if (lwM2MClientProfile != null) {
            profiles.put(deviceProfile.getUuidId(), lwM2MClientProfile);
            return true;
        }
        return false;
    }
}
