#include "../include/Orchestrator.h"

void OrchestratorClient::IncomingBuffer(const string &value) {
    mIncomingBuffer = value;
    mIncomingJSON.clear();

    try {
        mIncomingJSON = json::parse(mIncomingBuffer);
    } catch (...) {
        
    }
}

void OrchestratorClient::OutgoingBuffer(const string &value) {
    mOutgoingBuffer = value;
    mOutgoingJSON.clear();

    try {
        mOutgoingJSON = json::parse(mOutgoingBuffer);
    } catch (...) {

    }
}

void Orchestrator::init() {

}

std::string Orchestrator::generateRandomID() {
    std::string result;

    const std::string valid_characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<> distribution(0, valid_characters.size() - 1);

    for (int i = 0; i < 15; ++i) {
        result += valid_characters[distribution(generator)];
    }
    
    return result;
}

std::string Orchestrator::queryIPAddress(const char* mac_address) {
    if (Configuration.contains("Managed Devices") || Configuration["Managed Devices"].is_object()) {
        for (const auto& [device_id, device_info] : Configuration["Managed Devices"].items()) {
            if (device_info.value("MAC Address", "") == mac_address) {
                return device_info.value("IP Address", "IP Not Found");
            }
        }
    }
    
    return "IP Not Found";
}

std::string Orchestrator::queryMACAddress(const char* ip_address) {
    if (Configuration.contains("Managed Devices") || Configuration["Managed Devices"].is_object()) {
        for (const auto& [device_id, device_info] : Configuration["Managed Devices"].items()) {
            if (device_info.value("IP Address", "") == ip_address) {
                return device_info.value("MAC Address", "MAC Not Found");
            }
        }
    }
    
    return "MAC Not Found";
}

void Orchestrator::applyBindForUdpSocket(int sockfd) {
    if (mBindAddr.s_addr == 0) return;
    (void)setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, Configuration["Configuration"]["Bind"].get<string>().c_str(), (socklen_t)Configuration["Configuration"]["Bind"].get<string>().size());

    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_port = htons(0);
    local.sin_addr = mBindAddr;
    (void)bind(sockfd, (sockaddr*)&local, sizeof(local));
}

bool Orchestrator::sendMessage(const std::string& message, const uint16_t port, const char* dest_address) {
    int sockfd, broadcast = 1;
    struct sockaddr_in broadcast_addr;

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) return false;

    applyBindForUdpSocket(sockfd);

    if (setsockopt(sockfd, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast)) < 0) { close(sockfd); return false; }

    memset(&broadcast_addr, 0, sizeof(broadcast_addr));
    broadcast_addr.sin_family = AF_INET;
    broadcast_addr.sin_addr.s_addr = inet_addr(dest_address);
    broadcast_addr.sin_port = htons(port);

    if (sendto(sockfd, message.c_str(), message.length(), 0, (struct sockaddr *)&broadcast_addr, sizeof(broadcast_addr)) < 0) { close(sockfd); return false; }

    close(sockfd);
    return true;
}

void Orchestrator::serverListen(uint16_t port, uint16_t listen_timeout, std::function<void(Client)> ondata_callback, size_t bufferSize) {
    int server_fd;
    struct sockaddr_in address{};
    socklen_t addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        ServerLog->Write("[Listen] Socket", LOGLEVEL_ERROR);
        return;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        ServerLog->Write("[Listen] Bind", LOGLEVEL_ERROR);
        close(server_fd);
        return;
    }
    if (listen(server_fd, 1) < 0) {
        ServerLog->Write("[Listen] Listen", LOGLEVEL_ERROR);
        close(server_fd);
        return;
    }

    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(server_fd, &readfds);

    struct timeval timeout = {listen_timeout, 0};
    int activity = select(server_fd + 1, &readfds, nullptr, nullptr, &timeout);

    if (activity <= 0) {
        close(server_fd);
        return;
    }

    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(server_fd, (sockaddr*)&client_addr, &client_len);

    if (client_fd < 0) {
        ServerLog->Write("[Listen] Accept", LOGLEVEL_ERROR);
        close(server_fd);
        return;
    }

    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
    uint16_t client_port = ntohs(client_addr.sin_port);

    Client client;
    client.ID = client_fd;
    client.Info = client_addr;

    try {
        ondata_callback(client);
    } catch (...) {
        ServerLog->Write("[Listen] Callback error", LOGLEVEL_ERROR);
    }

    close(client_fd);
    close(server_fd);
}

void Orchestrator::serverMultipleListen(uint16_t port, uint16_t timeoutSeconds, std::function<void(Client)> callback, size_t bufferSize) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    
    if (server_fd == -1) {
        ServerLog->Write("[MultipleListem] Socket", LOGLEVEL_ERROR);
        return;
    }

    struct sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        ServerLog->Write("[MultipleListem] Bind", LOGLEVEL_ERROR);
        close(server_fd);
        return;
    }

    if (listen(server_fd, 10) < 0) {
        ServerLog->Write("[MultipleListem] Listen", LOGLEVEL_ERROR);
        close(server_fd);
        return;
    }

    fd_set readfds;
    struct timeval timeout {};
    timeout.tv_sec = timeoutSeconds;

    while (true) {
        FD_ZERO(&readfds);
        FD_SET(server_fd, &readfds);

        int activity = select(server_fd + 1, &readfds, nullptr, nullptr, &timeout);
        if (activity <= 0) break;

        if (FD_ISSET(server_fd, &readfds)) {
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
            if (client_fd >= 0) {
                std::string incoming;
                char buffer[bufferSize];
                ssize_t valread;

                while ((valread = recv(client_fd, buffer, sizeof(buffer), 0)) > 0) { incoming.append(buffer, valread); }

                Client client;
                client.ID = client_fd;
                client.IncomingBuffer = incoming;
                client.Info = client_addr;

                callback(client);
                close(client_fd);
            }
        }
    }

    close(server_fd);
}

void Orchestrator::List() {
    fprintf(stdout, "Devices managed by this server:\r\n\r\n");
    
    if (Configuration["Managed Devices"].size() == 0) {
        fprintf(stdout, "No devices managed by this server.\r\n\r\n");
        return;
    } else {
        fprintf(stdout, "%s | %s | %s | %s | %s | %s | %s | %s\r\n",
            String("Product Name").LimitString(20, true) .c_str(),
            String("Hardware Model").LimitString(20, true) .c_str(),
            String("Version").LimitString(8, true).c_str(),
            String("Device Name").LimitString(20, true) .c_str(),
            String("Hostname").LimitString(20, true).c_str(),
            String("MAC Address").LimitString(17, true).c_str(),
            String("IP Address").LimitString(15, true).c_str(),
            String("Last Update").LimitString(19, true).c_str()
        );

        if (Configuration.contains("Managed Devices") && Configuration["Managed Devices"].is_object()) {
            uint16_t c = 0;

            for (const auto& [device_id, device_info] : Configuration["Managed Devices"].items()) {
                c++;

                fprintf(stdout, "%s | %s | %s | %s | %s | %s | %s | %s\r\n",
                    String(device_info.value("Product Name", "Unknown")).LimitString(20, true).c_str(),
                    String(device_info.value("Hardware Model", "Unknown")).LimitString(20, true).c_str(),
                    String(device_info.value("Version", "Unknown")).LimitString(8, true).c_str(),
                    String(device_info.value("Device Name", "Unknown")).LimitString(20, true).c_str(),
                    String(device_info.value("Hostname", "Unknown")).LimitString(20, true).c_str(),
                    String(device_id).LimitString(17, true).c_str(),
                    String(device_info.value("IP Address", "Unknown")).LimitString(15, true).c_str(),
                    String(device_info.value("Last Update", 0)).LimitString(19, true).c_str()
                );
            }

            fprintf(stdout, "\r\n%d device%s managed.\r\n\r\n", c, (c > 1 ? "s" : ""));
        } else {
            fprintf(stdout, "Managed devices is missing or not an object.\r\n\r\n");
            exit(1);
        }
    }
}

void Orchestrator::Discovery(const DiscoveryMode mode, const uint16_t listen_timeout, const char* dest_address) {
    if (mBindAddr.s_addr == 0) return; //no Bind

    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();
    uint16_t mDiscoveredDevices = 0;

    json JsonReply = {
        {"Provider", "Orchestrator"},
        {"Request", "Discover"},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<string>()},
        {"Calling", mode == DISCOVERY_ALL ? "All" : (mode == DISCOVERY_MANAGED ? "Managed" : "Unmanaged")},
        {"Reply Port", port}
    };

    if (sendMessage(JsonReply.dump(), port, dest_address)) {
        fprintf(stdout, "Sending discovery for %s units - UDP (%s:%d)\r\n", JsonReply["Calling"].get<std::string>().c_str(), dest_address, port);
    } else {
        fprintf(stderr, "Error sending discovery call - UDP (%s:%d).\r\n\r\n", dest_address, port);
        return;
    }

    fprintf(stdout, "Listening on TCP port %d (timeout %d second%s)...\r\n\r\n", port, listen_timeout, (listen_timeout == 1 ? "" : "s"));
    fprintf(stdout, "%s | %s | %s | %s | %s | %s | %s\r\n",
        String("Product Name").LimitString(20, true).c_str(),
        String("Hardware Model").LimitString(20, true).c_str(),
        String("Version").LimitString(8, true).c_str(),
        String("Device Name").LimitString(20, true).c_str(),
        String("Hostname").LimitString(20, true).c_str(),
        String("MAC Address").LimitString(17, true).c_str(),
        String("IP Address").LimitString(15, true).c_str()
    );

    serverListen(port, listen_timeout, [&mDiscoveredDevices](Client client) {
        std::string incoming;
        char buffer[1024];
        ssize_t valread;

        struct timeval recv_timeout = {5, 0};
        setsockopt(client.ID, SOL_SOCKET, SO_RCVTIMEO, (const char*)&recv_timeout, sizeof(recv_timeout));

        while ((valread = recv(client.ID, buffer, sizeof(buffer), 0)) > 0) { incoming.append(buffer, valread); }

        if (incoming.empty()) return;

        try {
            json JsonIncoming = json::parse(incoming);
            const auto& info = JsonIncoming["DeviceIQ"];

            if (info.contains("MAC Address")) {
                mDiscoveredDevices++;

                auto getField = [&](const char* key, int limit, bool alignLeft = true) -> std::string {
                    return String(info.contains(key) ? info[key].get<std::string>() : "").LimitString(limit, alignLeft);
                };

                fprintf(stdout, "%s | %s | %s | %s | %s | %s | %s\r\n",
                    getField("Product Name", 20).c_str(),
                    getField("Hardware Model", 20).c_str(),
                    getField("Version", 8).c_str(),
                    getField("Device Name", 20).c_str(),
                    getField("Hostname", 20).c_str(),
                    getField("MAC Address", 17, false).c_str(),
                    String(inet_ntoa(client.Info.sin_addr)).LimitString(15, true).c_str()
                );
            }
        } catch (...) {

        }
    }, 0);

    if (mDiscoveredDevices == 0) {
        fprintf(stdout, "\r\nNo devices found.\r\n\r\n");
    } else {
        fprintf(stdout, "\r\n%d device%s found.\r\n\r\n", mDiscoveredDevices, (mDiscoveredDevices == 1 ? "" : "s"));
    }
}

OperationResult Orchestrator::Add(std::string target, const uint16_t listen_timeout, const bool force) {
    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();

    enum target_type { target_ip, target_mac };
    target_type t;

    if (target.find(':') != std::string::npos) {
        t = target_mac;

        if (Configuration.contains("Managed Devices") && Configuration["Managed Devices"].is_object()) {
            const auto& managedDevices = Configuration["Managed Devices"];
            if (managedDevices.contains(target)) return ADD_ALREADYMANAGED;
        }
    } else if (target.find('.') != std::string::npos) {
        t = target_ip;
    } else {
        return ADD_FAIL;
    }

    enum ListenStage { WAITING_DISCOVERY, WAITING_ADD_CONFIRMATION };
    ListenStage stage = WAITING_DISCOVERY;

    std::string resolved_ip, resolved_mac;
    json deviceInfo;
    OperationResult result = ADD_FAIL;
    bool addRequestSent = false;

    json discoveryRequest = {
        {"Provider", "Orchestrator"},
        {"Request", "Discover"},
        {"Calling", "All"},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<string>()},
        {"Reply Port", port}
    };

    if (!sendMessage(discoveryRequest.dump(), port, DEF_BROADCASTADDRESS)) { fprintf(stderr, "Error sending discovery call - UDP (%s:%d).\r\n\r\n", DEF_BROADCASTADDRESS, port); return ADD_FAIL; }

    serverMultipleListen(port, listen_timeout, [&](Client client) {
        try {
            json incoming = json::parse(client.IncomingBuffer);

            if (stage == WAITING_DISCOVERY) {
                if (!incoming.contains("DeviceIQ")) return;

                const auto& dev = incoming["DeviceIQ"];
                const std::string mac = dev.value("MAC Address", "");
                const std::string ip  = dev.value("IP Address", "");

                if ((t == target_mac && mac == target) || (t == target_ip && ip == target)) {
                    resolved_mac = mac;
                    resolved_ip = ip;
                    deviceInfo = dev;
                    stage = WAITING_ADD_CONFIRMATION;

                    json addRequest = {
                        {"Provider", "Orchestrator"},
                        {"Request", "Add"},
                        {"Server ID", Configuration["Configuration"]["Server ID"].get<string>()},
                        {"MAC Address", resolved_mac},
                        {"Reply Port", port}
                    };

                    if (!sendMessage(addRequest.dump(), port, resolved_ip.c_str())) { fprintf(stderr, "Error sending ADD request - UDP (%s:%d)\n", resolved_ip.c_str(), port); result = ADD_FAIL; return; }

                    addRequestSent = true;
                }
            }

            else if (stage == WAITING_ADD_CONFIRMATION && addRequestSent) {
                if (incoming.contains("Orchestrator") &&
                    incoming["Orchestrator"].value("Status", "") == "Added" &&
                    incoming.contains("DeviceIQ") &&
                    incoming["DeviceIQ"].value("MAC Address", "") == resolved_mac) {

                    const auto& dev = incoming["DeviceIQ"];

                    Configuration["Managed Devices"][resolved_mac] = {
                        {"Product Name", dev.value("Product Name", "")},
                        {"Hardware Model", dev.value("Hardware Model", "")},
                        {"Device Name", dev.value("Device Name", "")},
                        {"Version", dev.value("Version", "")},
                        {"Hostname", dev.value("Hostname", "")},
                        {"MAC Address", resolved_mac},
                        {"IP Address", resolved_ip},
                        {"Last Update", CurrentDateTime()}
                    };

                    result = SaveConfiguration() ? ADD_SUCCESS : ADD_FAIL;
                }
            }
        } catch (const std::exception& e) {
            
        }
    }, 1024);

    return result;
}

OperationResult Orchestrator::Remove(std::string target, const uint16_t listen_timeout, const bool force) {
    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();

    std::string ip_address, mac_address;

    if (target.find(':') != std::string::npos) {
        mac_address = target;
        ip_address = queryIPAddress(target.c_str());
    } else if (target.find('.') != std::string::npos) {
        mac_address = queryMACAddress(target.c_str());
        ip_address = target;
    } else {
        return REMOVE_FAIL;
    }

    if (!Configuration.contains("Managed Devices") || 
        !Configuration["Managed Devices"].is_object() || 
        !Configuration["Managed Devices"].contains(mac_address)) {
        return NOTMANAGED;
    }

    json request = {
        {"Provider", "Orchestrator"},
        {"Request", "Remove"},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<string>()},
        {"MAC Address", mac_address},
        {"Reply Port", port}
    };

    if (!sendMessage(request.dump(), port, ip_address.c_str())) {
        fprintf(stderr, "Error sending remove request - UDP (%s:%d).\r\n\r\n", ip_address.c_str(), port);
    } else {
        fprintf(stdout, "Sending remove request - UDP (%s:%d)\n", ip_address.c_str(), port);
    }

    OperationResult result = REMOVE_FAIL;
    bool replyReceived = false;

    serverListen(port, listen_timeout, [&](Client client) {
        std::string incoming;
        char buffer[1024];
        ssize_t valread;

        while ((valread = recv(client.ID, buffer, sizeof(buffer), 0)) > 0) { incoming.append(buffer, valread); }

        try {
            json response = json::parse(incoming);

            if (response.contains("Orchestrator") &&
                response["Orchestrator"].value("Status", "") == "Removed" &&
                response.contains("DeviceIQ") &&
                response["DeviceIQ"].value("MAC Address", "") == mac_address) {

                replyReceived = true;
                Configuration["Managed Devices"].erase(mac_address);
                result = SaveConfiguration() ? REMOVE_SUCCESS : REMOVE_FAIL;
            }
        } catch (...) {
            
        }
    }, 1024);

    if (!replyReceived && force) {
        fprintf(stdout, "[Remove] No confirmation from client. Forcing removal of %s\n", mac_address.c_str());
        Configuration["Managed Devices"].erase(mac_address);
        result = SaveConfiguration() ? REMOVE_SUCCESS : REMOVE_FAIL;
    }

    return result;
}

OperationResult Orchestrator::Refresh(std::string target, const uint16_t listen_timeout) {
    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();
    fprintf(stdout, "\r\n");

    if ((target == "all") || (target == "255.255.255.255")) {
        if (!Configuration.contains("Managed Devices") || !Configuration["Managed Devices"].is_object()) 
            return NOTMANAGED;

        std::map<std::string, json> discoveredDevices;

        json discoveryRequest = {
            {"Provider", "Orchestrator"},
            {"Request", "Discover"},
            {"Server ID", Configuration["Configuration"]["Server ID"].get<string>()},
            {"Calling", "All"},
            {"Reply Port", port}
        };

        if (!sendMessage(discoveryRequest.dump(), port, DEF_BROADCASTADDRESS)) { fprintf(stderr, "Error sending discovery call - UDP (%s:%d).\r\n\r\n", DEF_BROADCASTADDRESS, port); return REFRESH_FAIL; }
        
        serverListen(port, listen_timeout, [&](Client client) {
            std::string incoming;
            char buffer[1024];
            ssize_t valread;

            struct timeval recv_timeout = {5, 0};
            setsockopt(client.ID, SOL_SOCKET, SO_RCVTIMEO, (const char*)&recv_timeout, sizeof(recv_timeout));

            while ((valread = recv(client.ID, buffer, sizeof(buffer), 0)) > 0) { incoming.append(buffer, valread); }

            try {
                json incomingJson = json::parse(incoming);
                if (!incomingJson.contains("DeviceIQ")) return;

                const auto& dev = incomingJson["DeviceIQ"];
                std::string mac = dev.value("MAC Address", "");
                if (!mac.empty()) {
                    discoveredDevices[mac] = dev;
                }
            } catch (...) {
                
            }
        }, 0);

        int updatedCount = 0;
        for (auto& [mac, existing] : Configuration["Managed Devices"].items()) {
            if (discoveredDevices.count(mac)) {
                fprintf(stdout, "%s OK\r\n", mac.c_str());
                const auto& dev = discoveredDevices[mac];

                Configuration["Managed Devices"][mac] = {
                    {"Product Name", dev.value("Product Name", "")},
                    {"Hardware Model", dev.value("Hardware Model", "")},
                    {"Device Name", dev.value("Device Name", "")},
                    {"Version", dev.value("Version", "")},
                    {"Hostname", dev.value("Hostname", "")},
                    {"MAC Address", dev.value("MAC Address", "")},
                    {"IP Address", dev.value("IP Address", "")},
                    {"Last Update", CurrentDateTime()}
                };

                updatedCount++;
            } else {
                fprintf(stdout, "%s FAIL\r\n", mac.c_str());
            }
        }

        fprintf(stdout, "\r\n");
        if (updatedCount > 0) {
            if (!SaveConfiguration()) return REFRESH_FAIL;
            return (updatedCount == Configuration["Managed Devices"].size()) ? REFRESH_SUCCESS : REFRESH_PARTIAL;
        } else {
            return REFRESH_FAIL;
        }
    }

    enum target_type { target_ip, target_mac };
    target_type t;

    if (target.find(':') != std::string::npos) {
        t = target_mac;
        if (!Configuration.contains("Managed Devices") ||
            !Configuration["Managed Devices"].is_object() ||
            !Configuration["Managed Devices"].contains(target)) {
            return NOTMANAGED;
        }
    } else if (target.find('.') != std::string::npos) {
        t = target_ip;
    } else {
        return REFRESH_FAIL;
    }

    json discoveryRequest = {
        {"Provider", "Orchestrator"},
        {"Request", "Discover"},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<string>()},
        {"Calling", "All"},
        {"Reply Port", port}
    };

    json matchedDevice;
    std::string matched_mac;
    OperationResult result = REFRESH_FAIL;

    if (!sendMessage(discoveryRequest.dump(), port, DEF_BROADCASTADDRESS)) {
        fprintf(stderr, "Error sending discovery call - UDP (%s:%d).\r\n\r\n", DEF_BROADCASTADDRESS, port);
        return REFRESH_FAIL;
    }

    serverListen(port, listen_timeout, [&](Client client) {
        std::string incoming;
        char buffer[DEF_BUFFERSIZE];
        ssize_t valread;

        struct timeval recv_timeout = {5, 0};
        setsockopt(client.ID, SOL_SOCKET, SO_RCVTIMEO, (const char*)&recv_timeout, sizeof(recv_timeout));

        while ((valread = recv(client.ID, buffer, sizeof(buffer), 0)) > 0) { incoming.append(buffer, valread); }

        try {
            json incomingJson = json::parse(incoming);
            if (!incomingJson.contains("DeviceIQ")) return;

            const auto& dev = incomingJson["DeviceIQ"];
            std::string ip = dev.value("IP Address", "");
            std::string mac = dev.value("MAC Address", "");

            if ((t == target_mac && mac == target) || (t == target_ip && ip == target)) {
                matchedDevice = dev;
                matched_mac = mac;
                result = REFRESH_SUCCESS;
            }
        } catch (...) {
            
        }
    }, 0);

    if (result == REFRESH_SUCCESS) {
        fprintf(stdout, "%s OK\r\n\r\n", matched_mac.c_str());

        Configuration["Managed Devices"][matched_mac] = {
            {"Product Name", matchedDevice.value("Product Name", "")},
            {"Hardware Model", matchedDevice.value("Hardware Model", "")},
            {"Device Name", matchedDevice.value("Device Name", "")},
            {"Version", matchedDevice.value("Version", "")},
            {"Hostname", matchedDevice.value("Hostname", "")},
            {"MAC Address", matchedDevice.value("MAC Address", "")},
            {"IP Address", matchedDevice.value("IP Address", "")},
            {"Last Update", CurrentDateTime()}
        };

        result = SaveConfiguration() ? REFRESH_SUCCESS : REFRESH_FAIL;
    } else {
        fprintf(stdout, "%s FAIL\r\n\r\n", queryMACAddress(target.c_str()).c_str());
    }

    return result;
}

OperationResult Orchestrator::Pull(std::string target, const uint16_t listen_timeout) {
    if (!Configuration.contains("Managed Devices") || !Configuration["Managed Devices"].is_object())
        return NOTMANAGED;

    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();
    std::string ip, mac;

    if (target.find(':') != std::string::npos) {
        mac = target;
        ip = queryIPAddress(target.c_str());
    } else if (target.find('.') != std::string::npos) {
        ip = target;
        mac = queryMACAddress(target.c_str());
    } else {
        return PULLING_FAIL;
    }

    json JsonReply = {
        {"Provider", "Orchestrator"},
        {"Request", "Pull"},
        {"MAC Address", mac},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<string>()},
        {"Calling", "All"},
        {"Reply Port", port}
    };

    if (!sendMessage(JsonReply.dump(), port, ip.c_str())) {
        fprintf(stderr, "Error sending config pulling call - UDP (%s:%d).\r\n\r\n", ip.c_str(), port);
        return PULLING_FAIL;
    }

    OperationResult result = PULLING_FAIL;

    serverListen(port, listen_timeout, [&](Client client) {
        std::string incoming;
        char buffer[10240];
        ssize_t valread;

        struct timeval recv_timeout = {listen_timeout, 0};
        setsockopt(client.ID, SOL_SOCKET, SO_RCVTIMEO, (const char*)&recv_timeout, sizeof(recv_timeout));

        while ((valread = recv(client.ID, buffer, sizeof(buffer), 0)) > 0) { incoming.append(buffer, valread); }

        if (incoming.empty()) { result = PULLING_FAIL; return; }

        try {
            json JsonIncoming = json::parse(incoming);
            std::string mac = JsonIncoming["Network"]["MAC Address"];

            fprintf(stdout, "\r\n%s OK\r\n\r\n", mac.c_str());

            mac.erase(std::remove(mac.begin(), mac.end(), ':'), mac.end());
            std::string filename = mac + ".json";

            // Save to file with indentation
            std::ofstream ofs(filename);
            ofs << JsonIncoming.dump(4);  // Indented for readability
            ofs.close();

            result = PULLING_SUCCESS;
        } catch (const std::exception& e) {
            result = PULLING_FAIL;
        }
    }, 10240);

    return result;
}

OperationResult Orchestrator::Push(std::string target, const uint16_t listen_timeout, const bool apply) {
    if (!Configuration.contains("Managed Devices") || !Configuration["Managed Devices"].is_object())
        return NOTMANAGED;

    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();
    std::string ip, mac;

    if (target.find(':') != std::string::npos) {
        mac = target;
        ip = queryIPAddress(target.c_str());
    } else if (target.find('.') != std::string::npos) {
        ip = target;
        mac = queryMACAddress(target.c_str());
    } else {
        return PUSHING_FAIL;
    }

    // Load config file
    std::string config_filename = mac;
    config_filename.erase(std::remove(config_filename.begin(), config_filename.end(), ':'), config_filename.end());
    config_filename += ".json";

    std::ifstream ifs(config_filename);
    if (!ifs.is_open()) {
        fprintf(stderr, "[Push] Config file %s not found\n", config_filename.c_str());
        return NOTMANAGED;
    }

    std::stringstream buffer;
    buffer << ifs.rdbuf();
    std::string config_json_pretty = buffer.str();  // JSON identado (para leitura)
    ifs.close();

    // Compacta o JSON antes de enviar
    json parsed;
    try {
        parsed = json::parse(config_json_pretty);
    } catch (const std::exception& e) {
        fprintf(stderr, "[Push] Failed to parse saved config: %s\n", e.what());
        return PUSHING_FAIL;
    }

    std::string config_json = parsed.dump();  // Sem identação

    OperationResult result = PUSHING_FAIL;

    std::thread tcpListenerThread([&]() {
        fprintf(stdout, "[Push] Binding Listen() now... (Port %d)\n", port);
        serverListen(port, listen_timeout, [&](Client client) {
            fprintf(stdout, "[Push] Listen triggered!\n");

            std::string client_ip = inet_ntoa(client.Info.sin_addr);
            uint16_t client_port = ntohs(client.Info.sin_port);
            fprintf(stdout, "[Push] Accepted TCP connection from %s:%u\n", client_ip.c_str(), client_port);

            const char* data_ptr = config_json.c_str();
            ssize_t total_sent = 0;
            ssize_t remaining = config_json.size();

            while (remaining > 0) {
                ssize_t sent = send(client.ID, data_ptr + total_sent, remaining, 0);
                if (sent <= 0) {
                    perror("[Push] TCP send failed");       
                    return;
                }
                total_sent += sent;
                remaining -= sent;

                fprintf(stdout, "[Push] Sent %zd bytes, %zd remaining\n", sent, remaining);
            }

            fprintf(stdout, "[Push] Sent config (%zd bytes) to client\n", total_sent);

            shutdown(client.ID, SHUT_WR);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));  // tempo para o ESP ler

            // Espera por ACK
            char ack_buf[64] = {0};
            ssize_t ack_len = recv(client.ID, ack_buf, sizeof(ack_buf) - 1, 0);
            if (ack_len > 0) {
                ack_buf[ack_len] = '\0';
                fprintf(stdout, "[Push] Client ACK: %s\n", ack_buf);
            } else {
                fprintf(stdout, "[Push] No ACK received\n");
            }

            result = PUSHING_SUCCESS;
        }, 0);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(250)); // aguarda listener

    // Envia comando Push via UDP
    json JsonReply = {
        {"Provider", "Orchestrator"},
        {"Request", "Push"},
        {"Apply", apply},
        {"MAC Address", mac},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<string>()},
        {"Calling", "All"},
        {"Reply Port", port}
    };

    if (!sendMessage(JsonReply.dump(), port, ip.c_str())) {
        fprintf(stderr, "Error sending config pushing call - UDP (%s:%d).\r\n\r\n", ip.c_str(), port);
        return PUSHING_FAIL;
    }

    tcpListenerThread.join();
    return result;
}

bool Orchestrator::Update(std::string target) {
    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();

    std::string ip_address, mac_address;

    if (target.find(':') != std::string::npos) {
        mac_address = target;
        ip_address = queryIPAddress(target.c_str());
    } else if (target.find('.') != std::string::npos) {
        mac_address = queryMACAddress(target.c_str());
        ip_address = target;
    } else {
        return false;
    }

    if (Configuration.contains("Managed Devices") && Configuration["Managed Devices"].is_object()) {
        const auto& managedDevices = Configuration["Managed Devices"];
        if (managedDevices.size() == 0) {
            fprintf(stderr, "No devices managed by this Orchestrator.\r\n\r\n");
            exit(1);
        } else {
            if (managedDevices.contains(mac_address)) {
                json JsonReply;
        
                JsonReply["Provider"] = "Orchestrator";
                JsonReply["Request"] = "Update";
                JsonReply["Server ID"] = Configuration["Configuration"]["Server ID"].get<string>();
                JsonReply["MAC Address"] = mac_address;

                if (sendMessage(JsonReply.dump(), Configuration["Configuration"]["Port"], ip_address.c_str()) == true) {
                    fprintf(stdout, "Sending restart request to %s - UDP (%s:%d).\r\n\r\n", mac_address.c_str(), ip_address.c_str(), port);
                    return true;
                } else {
                    fprintf(stderr, "Error sending restart request to %s - UDP (%s:%d).\r\n\r\n", mac_address.c_str(), ip_address.c_str(), port);
                    exit(1);
                }
            } else {
                fprintf(stderr, "Device %s is not managed by this Orchestrator.\r\n\r\n", mac_address.c_str());
                exit(1);
            }
        }
    }

    return false;
}

bool Orchestrator::Restart(std::string target) {
    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();

    std::string ip_address, mac_address;

    if (target.find(':') != std::string::npos) {
        mac_address = target;
        ip_address = queryIPAddress(target.c_str());
    } else if (target.find('.') != std::string::npos) {
        mac_address = queryMACAddress(target.c_str());
        ip_address = target;
    } else {
        return false;
    }

    if (Configuration.contains("Managed Devices") && Configuration["Managed Devices"].is_object()) {
        const auto& managedDevices = Configuration["Managed Devices"];
        if (managedDevices.size() == 0) {
            fprintf(stderr, "No devices managed by this Orchestrator.\r\n\r\n");
            exit(1);
        } else {
            if (managedDevices.contains(mac_address)) {
                json JsonReply;
        
                JsonReply["Provider"] = "Orchestrator";
                JsonReply["Request"] = "Restart";
                JsonReply["Server ID"] = Configuration["Configuration"]["Server ID"].get<string>();
                JsonReply["MAC Address"] = mac_address;

                if (sendMessage(JsonReply.dump(), Configuration["Configuration"]["Port"], ip_address.c_str()) == true) {
                    fprintf(stdout, "Sending restart request to %s - UDP (%s:%d).\r\n\r\n", mac_address.c_str(), ip_address.c_str(), port);
                    return true;
                } else {
                    fprintf(stderr, "Error sending restart request to %s - UDP (%s:%d).\r\n\r\n", mac_address.c_str(), ip_address.c_str(), port);
                    exit(1);
                }
            } else {
                fprintf(stderr, "Device %s is not managed by this Orchestrator.\r\n\r\n", mac_address.c_str());
                exit(1);
            }
        }
    }

    return false;
}

bool Orchestrator::Initialize() {
    bool ret = false;

    if (Configuration.empty()) {
        if (ReadConfiguration()) {
            if (JSON<std::string>(Configuration["Configuration"]["Server Name"], "") == "") Configuration["Configuration"]["Server Name"] = DEF_SERVERNAME;
            if (JSON<std::string>(Configuration["Configuration"]["Server ID"], "") == "") Configuration["Configuration"]["Server ID"] = generateRandomID();
        }
    }

    // Bind
    if (setBindInterface(JSON<std::string>(Configuration["Configuration"]["Bind"], ""))) {
        ret = true;
    } else {
        ret = false;
    }

    return ret;
}

bool Orchestrator::ReadConfiguration() {
    bool ret = false;

    if (mConfigFile.empty()) {
        char exePath[PATH_MAX];

        ssize_t count = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
        if (count != -1) {
            exePath[count] = '\0';
            mConfigFile = "./" + Version.ProductName + ".json";
        } else {
            mConfigFile = DEF_CONFIGFILE;
        }
    }

    std::ifstream configFile(mConfigFile);

    if (configFile.is_open() == true) {
        try {
            configFile >> Configuration;
            ret = true;
        } catch (nlohmann::json::parse_error& ex) {
            // ServerLog->Write("Parse error at byte " + to_string(ex.byte) + ":" + ex.what() + " on file " + mConfigFile, LOGLEVEL_ERROR);
            ret = false;
        }
    } else {
        Configuration["Configuration"] = {
            {"Log Endpoint", "ENDPOINT_FILE, ENDPOINT_SYSLOG_LOCAL, ENDPOINT_SYSLOG_REMOTE"},
            {"Log File Append", false},
            {"Bind", ""},
            {"Port", 30030},
            {"Timeout Ms", 15000},
            {"Buffer Size", 1024},
            {"Server ID", ""},
            {"Server Name", ""},
            {"Syslog Port", 514},
            {"Syslog URL", ""}
        };

        Configuration["Managed Devices"] = {
        };
        
        if (SaveConfiguration()) {
            // ServerLog->Write("Configuration file not found - Default config file " + string(DEF_CONFIGFILE) + " created", LOGLEVEL_WARNING);
            ret = true;
        } else {
            // ServerLog->Write("Configuration file not found - Unable to create default config file " + string(DEF_CONFIGFILE), LOGLEVEL_ERROR);
            ret = false;
        }
    }

    return ret;
}

bool Orchestrator::SaveConfiguration() {
    ofstream outFile(mConfigFile.c_str());

    if (!outFile.is_open()) return false;

    outFile << Configuration.dump(4);
    outFile.close();

    return !outFile.fail();
}

bool Orchestrator::resolveInterfaceOrIp(const std::string& ifaceOrIp, in_addr& out) {
    in_addr tmp{};
    if (inet_aton(ifaceOrIp.c_str(), &tmp)) { out = tmp; return true; }

    ifaddrs* ifaddr = nullptr;
    if (getifaddrs(&ifaddr) == -1) return false;

    bool ok = false;
    for (ifaddrs* ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr) continue;
        if (ifa->ifa_addr->sa_family != AF_INET) continue;
        if (ifaceOrIp != ifa->ifa_name) continue;
        out = ((sockaddr_in*)ifa->ifa_addr)->sin_addr;
        ok = true;
        break;
    }
    freeifaddrs(ifaddr);
    return ok;
}

bool Orchestrator::setBindInterface(const std::string& ifaceOrIp) {
    if (ifaceOrIp.empty()) {
        mBindAddr = {};
        return true;
    }

    in_addr addr{};
    if (!resolveInterfaceOrIp(ifaceOrIp, addr)) return false;

    mBindAddr = addr;
    
    return true;
}

int Orchestrator::Manage() {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGUSR1);
    sigaddset(&mask, SIGUSR2);
    sigaddset(&mask, SIGPIPE); // prevent SIGPIPE to kill process

    if (pthread_sigmask(SIG_BLOCK, &mask, nullptr) != 0) {
        ServerLog->Write("[Manager] pthread_sigmask", LOGLEVEL_ERROR);
        return 1;
    }

    int signal_fd = signalfd(-1, &mask, SFD_CLOEXEC);
    if (signal_fd == -1) {
        ServerLog->Write("[Manager] signalfd", LOGLEVEL_ERROR);
        return 1;
    }

    int manager_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (manager_fd == -1) {
        ServerLog->Write("[Manager] Socket", LOGLEVEL_ERROR);
        close(signal_fd);
        return 1;
    }

    int opt = 1;
    if (setsockopt(manager_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        ServerLog->Write("[Manager] setsockopt(SO_REUSEADDR)", LOGLEVEL_ERROR);
        close(manager_fd);
        close(signal_fd);
        return 1;
    }

    if (mBindAddr.s_addr != 0) {
        const std::string iface = JSON<std::string>(Configuration["Configuration"]["Bind"], "");
        if (!iface.empty()) {
            if (setsockopt(manager_fd, SOL_SOCKET, SO_BINDTODEVICE, iface.c_str(), (socklen_t)iface.size()) < 0) {
                ServerLog->Write("[Manager] setsockopt(SO_BINDTODEVICE) falhou; prosseguindo com bind por IP", LOGLEVEL_WARNING);
            }
        }
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(Configuration["Configuration"]["Port"].get<uint16_t>());
    address.sin_addr = (mBindAddr.s_addr != 0) ? mBindAddr : in_addr{ htonl(INADDR_ANY) };

    if (bind(manager_fd, (sockaddr*)&address, sizeof(address)) < 0) {
        ServerLog->Write("[Manager] Bind", LOGLEVEL_ERROR);
        close(manager_fd);
        close(signal_fd);
        return 1;
    }

    if (listen(manager_fd, 10) < 0) {
        ServerLog->Write("[Manager] Listen", LOGLEVEL_ERROR);
        close(manager_fd);
        close(signal_fd);
        return 1;
    }

    const size_t buf_sz = Configuration["Configuration"]["Buffer Size"].get<size_t>();

    ServerLog->Write("Orchestrator Manager is running (pid " + String(getpid()) + ")", LOGLEVEL_INFO);

    bool running = true;
    while (running) {
        struct pollfd pfds[2];
        pfds[0].fd = signal_fd;
        pfds[0].events = POLLIN;
        pfds[1].fd = manager_fd;
        pfds[1].events = POLLIN;

        int pr = poll(pfds, 2, -1);
        if (pr < 0) {
            if (errno == EINTR) continue;
            ServerLog->Write("[Manager] Poll", LOGLEVEL_ERROR);
            break;
        }

        if (pfds[0].revents & POLLIN) {
            signalfd_siginfo si{};
            ssize_t n = read(signal_fd, &si, sizeof(si));
            if (n == sizeof(si)) {
                switch (si.ssi_signo) {
                    case SIGINT:
                        ServerLog->Write("SIGINT (Ctrl+C)", LOGLEVEL_INFO);
                        running = false;
                        break;
                    case SIGTERM:
                        ServerLog->Write("SIGTERM", LOGLEVEL_INFO);
                        running = false;
                        break;
                    case SIGHUP:
                        ServerLog->Write("Reload configuration file " + mConfigFile, LOGLEVEL_INFO);
                        ReadConfiguration();
                        break;
                    case SIGUSR1:
                        ServerLog->Write("SIGUSR1", LOGLEVEL_INFO);
                        break;
                    case SIGUSR2:
                        ServerLog->Write("SIGUSR2", LOGLEVEL_INFO);
                        break;
                    case SIGPIPE:
                        ServerLog->Write("SIGPIPE", LOGLEVEL_INFO);
                        break;
                    default:
                        ServerLog->Write("Signal not addressed: " + String(si.ssi_signo), LOGLEVEL_INFO);
                        break;
                }
            } else {
                ServerLog->Write("[Manager] read(signalfd) short read", LOGLEVEL_ERROR);
            }
        }

        if (pfds[1].revents & POLLIN) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(manager_fd, (sockaddr*)&client_addr, &client_len);
            if (client_fd >= 0) {
                std::string incoming;
                std::vector<char> buffer(buf_sz);

                while (true) {
                    ssize_t r = recv(client_fd, buffer.data(), buffer.size(), 0);
                    if (r > 0) {
                        incoming.append(buffer.data(), static_cast<size_t>(r));
                    } else if (r == 0) {
                        // peer closed
                        break;
                    } else {
                        if (errno == EINTR) continue;
                        ServerLog->Write("[Manager] recv", LOGLEVEL_ERROR);
                        break;
                    }
                }

                OrchestratorClient *client = new OrchestratorClient(client_fd, client_addr);
                client->IncomingBuffer(incoming);

                if ((client->IncomingJSON().contains("Orchestrator")) && (client->IncomingJSON()["Orchestrator"].value("Version", "") == Version.Software.Info())) {
                    ServerLog->Write("Valid Protocol", LOGLEVEL_INFO);
                } else {
                    ServerLog->Write("Protocol Error - Incoming Buffer [" + client->IncomingBuffer() + "]", LOGLEVEL_ERROR);
                }
                
                // if (incoming.contains("Orchestrator") &&
                //     incoming["Orchestrator"].value("Status", "") == "Added" &&
                //     incoming.contains("DeviceIQ") &&
                //     incoming["DeviceIQ"].value("MAC Address", "") == resolved_mac) {

                //     const auto& dev = incoming["DeviceIQ"];

                //     Configuration["Managed Devices"][resolved_mac] = {
                //         {"Product Name", dev.value("Product Name", "")},
                //         {"Hardware Model", dev.value("Hardware Model", "")},
                //         {"Device Name", dev.value("Device Name", "")},
                //         {"Version", dev.value("Version", "")},
                //         {"Hostname", dev.value("Hostname", "")},
                //         {"MAC Address", resolved_mac},
                //         {"IP Address", resolved_ip},
                //         {"Last Update", CurrentDateTime()}
                //     };

                //     result = SaveConfiguration() ? ADD_SUCCESS : ADD_FAIL;
                // }

                close(client_fd);
            } else {
                ServerLog->Write("[Manager] accept", LOGLEVEL_ERROR);
            }
        }
    }

    close(manager_fd);
    close(signal_fd);

    ServerLog->Write("Orchestrator Manager finished", LOGLEVEL_INFO);
    return 0;
}