package com.alok.iot.mqtt;

import com.alok.iot.mqtt.gcp.MqttExampleOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import static com.alok.iot.mqtt.gcp.MqttExample.*;

@SpringBootApplication
public class IotConnectApplication {

	public static void main(String[] args) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException, MqttException, InterruptedException {
		SpringApplication.run(IotConnectApplication.class, args);

        MqttExampleOptions options = MqttExampleOptions.fromFlags(args);
        if (options == null) {
            // Could not parse.
            System.exit(1);
        }

        if ("listen-for-config-messages".equals(options.command)) {
            System.out.println(
                    String.format("Listening for configuration messages for %s:", options.deviceId));
            listenForConfigMessages(
                    options.mqttBridgeHostname,
                    options.mqttBridgePort,
                    options.projectId,
                    options.cloudRegion,
                    options.registryId,
                    options.gatewayId,
                    options.privateKeyFile,
                    options.algorithm,
                    options.deviceId);
        } else if ("send-data-from-bound-device".equals(options.command)) {
            System.out.println("Sending data on behalf of device:");
            sendDataFromBoundDevice(
                    options.mqttBridgeHostname,
                    options.mqttBridgePort,
                    options.projectId,
                    options.cloudRegion,
                    options.registryId,
                    options.gatewayId,
                    options.privateKeyFile,
                    options.algorithm,
                    options.deviceId,
                    options.messageType,
                    options.telemetryData);
        } else {
            System.out.println("Starting mqtt demo:");
            mqttDeviceDemo(options);
        }
        // [END iot_mqtt_configuremqtt]
    }
}
