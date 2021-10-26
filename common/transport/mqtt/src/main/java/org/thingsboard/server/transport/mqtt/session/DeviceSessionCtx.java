/**
 * Copyright © 2016-2021 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.mqtt.session;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.util.ReferenceCountUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.TransportPayloadType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.MqttDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.ProtoTransportPayloadConfiguration;
import org.thingsboard.server.common.data.device.profile.TransportPayloadTypeConfiguration;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.mqtt.MqttTransportContext;
import org.thingsboard.server.transport.mqtt.adaptors.BackwardCompatibilityAdaptor;
import org.thingsboard.server.transport.mqtt.adaptors.MqttTransportAdaptor;
import org.thingsboard.server.transport.mqtt.util.MqttTopicFilter;
import org.thingsboard.server.transport.mqtt.util.MqttTopicFilterFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * @author Andrew Shvayka
 */
@Slf4j
public class DeviceSessionCtx extends MqttDeviceAwareSessionContext {

    @Getter
    @Setter
    private ChannelHandlerContext channel;

    @Getter
    private final MqttTransportContext context;

    private final AtomicInteger msgIdSeq = new AtomicInteger(0);

    private final ConcurrentLinkedQueue<MqttMessage> msgQueue = new ConcurrentLinkedQueue<>();

    @Getter
    private final Lock msgQueueProcessorLock = new ReentrantLock();

    private final AtomicInteger msgQueueSize = new AtomicInteger(0);

    @Getter
    @Setter
    private boolean provisionOnly = false;

    private volatile MqttTopicFilter telemetryTopicFilter = MqttTopicFilterFactory.getDefaultTelemetryFilter();
    private volatile MqttTopicFilter attributesTopicFilter = MqttTopicFilterFactory.getDefaultAttributesFilter();
    private volatile TransportPayloadType payloadType = TransportPayloadType.JSON;
    private volatile boolean payloadFormatsCompatipilityEnabled;
    private volatile Descriptors.Descriptor attributesDynamicMessageDescriptor;
    private volatile Descriptors.Descriptor telemetryDynamicMessageDescriptor;
    private volatile Descriptors.Descriptor rpcResponseDynamicMessageDescriptor;
    private volatile DynamicMessage.Builder rpcRequestDynamicMessageBuilder;

    @Getter
    @Setter
    private TransportPayloadType provisionPayloadType = payloadType;

    public DeviceSessionCtx(UUID sessionId, ConcurrentMap<MqttTopicMatcher, Integer> mqttQoSMap, MqttTransportContext context) {
        super(sessionId, mqttQoSMap);
        this.context = context;
    }

    public int nextMsgId() {
        return msgIdSeq.incrementAndGet();
    }

    public boolean isDeviceTelemetryTopic(String topicName) {
        return telemetryTopicFilter.filter(topicName);
    }

    public boolean isDeviceAttributesTopic(String topicName) {
        return attributesTopicFilter.filter(topicName);
    }

    public MqttTransportAdaptor getPayloadAdaptor() {
        if (payloadType.equals(TransportPayloadType.JSON)) {
            return context.getJsonMqttAdaptor();
        } else {
            if (payloadFormatsCompatipilityEnabled) {
                return new BackwardCompatibilityAdaptor(context.getProtoMqttAdaptor(), context.getJsonMqttAdaptor());
            } else {
                return context.getProtoMqttAdaptor();
            }
        }
    }

    public boolean isJsonPayloadType() {
        return payloadType.equals(TransportPayloadType.JSON);
    }

    public Descriptors.Descriptor getTelemetryDynamicMsgDescriptor() {
        return telemetryDynamicMessageDescriptor;
    }

    public Descriptors.Descriptor getAttributesDynamicMessageDescriptor() {
        return attributesDynamicMessageDescriptor;
    }

    public Descriptors.Descriptor getRpcResponseDynamicMessageDescriptor() {
        return rpcResponseDynamicMessageDescriptor;
    }

    public DynamicMessage.Builder getRpcRequestDynamicMessageBuilder() {
        return rpcRequestDynamicMessageBuilder;
    }

    @Override
    public void setDeviceProfile(DeviceProfile deviceProfile) {
        super.setDeviceProfile(deviceProfile);
        updateTopicFilters(deviceProfile);
    }

    @Override
    public void onDeviceProfileUpdate(TransportProtos.SessionInfoProto sessionInfo, DeviceProfile deviceProfile) {
        super.onDeviceProfileUpdate(sessionInfo, deviceProfile);
        updateTopicFilters(deviceProfile);
    }

    private void updateTopicFilters(DeviceProfile deviceProfile) {
        DeviceProfileTransportConfiguration transportConfiguration = deviceProfile.getProfileData().getTransportConfiguration();
        if (transportConfiguration.getType().equals(DeviceTransportType.MQTT) &&
                transportConfiguration instanceof MqttDeviceProfileTransportConfiguration) {
            MqttDeviceProfileTransportConfiguration mqttConfig = (MqttDeviceProfileTransportConfiguration) transportConfiguration;
            TransportPayloadTypeConfiguration transportPayloadTypeConfiguration = mqttConfig.getTransportPayloadTypeConfiguration();
            payloadType = transportPayloadTypeConfiguration.getTransportPayloadType();
            telemetryTopicFilter = MqttTopicFilterFactory.toFilter(mqttConfig.getDeviceTelemetryTopic());
            attributesTopicFilter = MqttTopicFilterFactory.toFilter(mqttConfig.getDeviceAttributesTopic());
            if (TransportPayloadType.PROTOBUF.equals(payloadType)) {
                updateDynamicMessageDescriptors(transportPayloadTypeConfiguration);
            }
        } else {
            telemetryTopicFilter = MqttTopicFilterFactory.getDefaultTelemetryFilter();
            attributesTopicFilter = MqttTopicFilterFactory.getDefaultAttributesFilter();
        }
    }

    private void updateDynamicMessageDescriptors(TransportPayloadTypeConfiguration transportPayloadTypeConfiguration) {
        ProtoTransportPayloadConfiguration protoTransportPayloadConfig = (ProtoTransportPayloadConfiguration) transportPayloadTypeConfiguration;
        telemetryDynamicMessageDescriptor = protoTransportPayloadConfig.getTelemetryDynamicMessageDescriptor(protoTransportPayloadConfig.getDeviceTelemetryProtoSchema());
        attributesDynamicMessageDescriptor = protoTransportPayloadConfig.getAttributesDynamicMessageDescriptor(protoTransportPayloadConfig.getDeviceAttributesProtoSchema());
        rpcResponseDynamicMessageDescriptor = protoTransportPayloadConfig.getRpcResponseDynamicMessageDescriptor(protoTransportPayloadConfig.getDeviceRpcResponseProtoSchema());
        rpcRequestDynamicMessageBuilder = protoTransportPayloadConfig.getRpcRequestDynamicMessageBuilder(protoTransportPayloadConfig.getDeviceRpcRequestProtoSchema());
        payloadFormatsCompatipilityEnabled = protoTransportPayloadConfig.isEnableCompatibilityWithOtherPayloadFormats();
    }

    public void addToQueue(MqttMessage msg) {
        msgQueueSize.incrementAndGet();
        ReferenceCountUtil.retain(msg);
        msgQueue.add(msg);
    }

    public void tryProcessQueuedMsgs(Consumer<MqttMessage> msgProcessor) {
        while (!msgQueue.isEmpty()) {
            if (msgQueueProcessorLock.tryLock()) {
                try {
                    MqttMessage msg;
                    while ((msg = msgQueue.poll()) != null) {
                        try {
                            msgQueueSize.decrementAndGet();
                            msgProcessor.accept(msg);
                        } finally {
                            ReferenceCountUtil.safeRelease(msg);
                        }
                    }
                } finally {
                    msgQueueProcessorLock.unlock();
                }
            } else {
                return;
            }
        }
    }

    public int getMsgQueueSize() {
        return msgQueueSize.get();
    }

    public void release() {
        if (!msgQueue.isEmpty()) {
            log.warn("doDisconnect for device {} but unprocessed messages {} left in the msg queue", getDeviceId(), msgQueue.size());
            msgQueue.forEach(ReferenceCountUtil::safeRelease);
            msgQueue.clear();
        }
    }

    public Collection<MqttMessage> getMsgQueueSnapshot(){
        return Collections.unmodifiableCollection(msgQueue);
    }

}
