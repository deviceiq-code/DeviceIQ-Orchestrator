#include "../include/Orchestrator.h"


bool OrchestratorClient::Reply() {
    if (mID <= 0) return false;
    if (mOutgoingJSON.empty()) return true;

    string replymessage = mOutgoingJSON.dump(-1);

    const char* data = replymessage.data();
    size_t total_sent = 0;
    size_t remaining  = replymessage.size();

    constexpr int SEND_TIMEOUT_MS = 3000;

    while (remaining > 0) {
        ssize_t n = ::send(mID, data + total_sent, remaining, MSG_NOSIGNAL);
        if (n > 0) {
            total_sent += static_cast<size_t>(n);
            remaining -= static_cast<size_t>(n);
            continue;
        }
        if (n == -1) {
            if (errno == EINTR) continue;

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd pfd{mID, POLLOUT, 0 };
                int pr = ::poll(&pfd, 1, SEND_TIMEOUT_MS);
                if (pr > 0) continue;
                return false;
            }
            return false;
        }
    }
    return true;
}

void OrchestratorClient::IncomingBuffer(const string &value) {
    mIncomingBuffer = value;
    mIncomingJSON.clear();

    try {
        mIncomingJSON = json::parse(mIncomingBuffer);
    } catch (...) {
        
    }
}

void OrchestratorClient::OutgoingBuffer(const json &value) {
    OutgoingBuffer(value.dump());
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
    mServerStartedTimestamp = CurrentDateTime();
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

// OperationResult Orchestrator::Pull(std::string target, const uint16_t listen_timeout) {
//     if (!Configuration.contains("Managed Devices") || !Configuration["Managed Devices"].is_object())
//         return NOTMANAGED;

//     const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();
//     std::string ip, mac;

//     if (target.find(':') != std::string::npos) {
//         mac = target;
//         ip = queryIPAddress(target.c_str());
//     } else if (target.find('.') != std::string::npos) {
//         ip = target;
//         mac = queryMACAddress(target.c_str());
//     } else {
//         return PULLING_FAIL;
//     }

//     json JsonReply = {
//         {"Provider", "Orchestrator"},
//         {"Request", "Pull"},
//         {"MAC Address", mac},
//         {"Server ID", Configuration["Configuration"]["Server ID"].get<string>()},
//         {"Calling", "All"},
//         {"Reply Port", port}
//     };

//     if (!sendMessage(JsonReply.dump(), port, ip.c_str())) {
//         fprintf(stderr, "Error sending config pulling call - UDP (%s:%d).\r\n\r\n", ip.c_str(), port);
//         return PULLING_FAIL;
//     }

//     OperationResult result = PULLING_FAIL;

//     serverListen(port, listen_timeout, [&](Client client) {
//         std::string incoming;
//         char buffer[10240];
//         ssize_t valread;

//         struct timeval recv_timeout = {listen_timeout, 0};
//         setsockopt(client.ID, SOL_SOCKET, SO_RCVTIMEO, (const char*)&recv_timeout, sizeof(recv_timeout));

//         while ((valread = recv(client.ID, buffer, sizeof(buffer), 0)) > 0) { incoming.append(buffer, valread); }

//         if (incoming.empty()) { result = PULLING_FAIL; return; }

//         try {
//             json JsonIncoming = json::parse(incoming);
//             std::string mac = JsonIncoming["Network"]["MAC Address"];

//             fprintf(stdout, "\r\n%s OK\r\n\r\n", mac.c_str());

//             mac.erase(std::remove(mac.begin(), mac.end(), ':'), mac.end());
//             std::string filename = mac + ".json";

//             // Save to file with indentation
//             std::ofstream ofs(filename);
//             ofs << JsonIncoming.dump(4);  // Indented for readability
//             ofs.close();

//             result = PULLING_SUCCESS;
//         } catch (const std::exception& e) {
//             result = PULLING_FAIL;
//         }
//     }, 10240);

//     return result;
// }

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

bool Orchestrator::SaveDeviceConfiguration(const json &config) {
    std::string mac = config["Network"]["MAC Address"].get<std::string>();
    mac.erase(std::remove(mac.begin(), mac.end(), ':'), mac.end());

    std::string config_file = "./config/" + mac + ".json";

    filesystem::create_directories("./config");

    std::ofstream outFile(config_file);
    if (!outFile.is_open()) return false;

    outFile << config.dump(4);
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

json Orchestrator::Query(const std::string& orchestrator_url, uint16_t orchestrator_port, const json& payload) {
    json reply;
    
    if (payload.empty()) return false;
    string dumped = payload.dump(-1);

    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%u", orchestrator_port);

    struct addrinfo* res = nullptr;
    int gai = getaddrinfo(orchestrator_url.c_str(), portstr, &hints, &res);
    if (gai != 0 || !res) return false;

    const int CONNECT_TIMEOUT_MS = 700; // connect
    const int READ_TIMEOUT_SEC = 1; // recv

    for (auto* rp = res; rp != nullptr; rp = rp->ai_next) {
        int fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) continue;

        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        int rc = connect(fd, rp->ai_addr, rp->ai_addrlen);
        if (rc < 0 && errno != EINPROGRESS && errno != EALREADY) { close(fd); continue; }

        struct pollfd pfd{ fd, POLLOUT, 0 };
        int pr = poll(&pfd, 1, CONNECT_TIMEOUT_MS);
        if (pr <= 0) { close(fd); continue; }

        int soerr = 0; socklen_t len = sizeof(soerr);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &len) < 0 || soerr != 0) { close(fd); continue; }

        fcntl(fd, F_SETFL, flags);

        timeval tv{ READ_TIMEOUT_SEC, 0 };
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        const char* p = dumped.c_str();
        size_t to_send = dumped.size();

        while (to_send > 0) {
            ssize_t n = send(fd, p, to_send, 0);
            if (n < 0) {
                if (errno == EINTR) continue;
                close(fd);
                fd = -1;
                break;
            }
            p += n;
            to_send -= static_cast<size_t>(n);
        }
        if (fd < 0) continue;

        shutdown(fd, SHUT_WR);

        std::string incoming;
        char buf[4096];
        while (true) {
            ssize_t r = recv(fd, buf, sizeof(buf), 0);
            if (r > 0) { incoming.append(buf, static_cast<size_t>(r)); }
            else if (r == 0) { break; }
            else {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                incoming.clear();
                break;
            }
        }
        close(fd);

        try {
            if (!incoming.empty()) reply = json::parse(incoming);
        } catch (...) {
            
        }
        break;
    }

    freeaddrinfo(res);
    return reply;
}

void Orchestrator::UpdateStatus(bool status) {
    json JsonStatusUpdater;
    JsonStatusUpdater["Orchestrator"]["Status"] = {
        {"Version", Version.Software.Info()},
        {"Online", status},
        {"Server Started", ServerStartedTimestamp()},
        {"Last Update", CurrentDateTime()}
    };

    ofstream outFile(JSON<string>(Configuration["Configuration"]["Status Updater"]["Output File"], "./status.json").c_str());
    if (!outFile.is_open()) {
        ServerLog->Write("Failed to open Orchestrator status update file " + JSON<string>(Configuration["Configuration"]["Status Updater"]["Output File"], "./status.json"), LOGLEVEL_ERROR);
    };

    outFile << JsonStatusUpdater.dump(4);
    outFile.close();

    if (!outFile.fail()) {
        ServerLog->Write("Orchestrator status file " + JSON<string>(Configuration["Configuration"]["Status Updater"]["Output File"], "./status.json") + " updated", LOGLEVEL_INFO);
    }
}

bool Orchestrator::CheckOnline(const std::string& orchestrator_url, uint16_t orchestrator_port) {
    json JsonQuery;
    JsonQuery = {
        {"Provider", "Orchestrator"},
        {"Command", "CheckOnline"},
        {"Parameter", ""}
    };

    json JsonReply = Query(orchestrator_url, orchestrator_port, JsonQuery);
    bool rst = false;

    if (!JsonReply.empty()) {
        if (JsonReply.contains("Provider")) {
            rst = (JsonReply.value("Result", "") == "Yes" ? true : false);
        }
    }
    return rst;
}

bool Orchestrator::ReloadConfig(const std::string& orchestrator_url, uint16_t orchestrator_port) {
    json JsonQuery;
    JsonQuery = {
        {"Provider", "Orchestrator"},
        {"Command", "ReloadConfig"},
        {"Parameter", ""}
    };

    json JsonReply = Query(orchestrator_url, orchestrator_port, JsonQuery);
    bool rst = false;

    if (!JsonReply.empty()) {
        if (JsonReply.contains("Provider")) {
            rst = (JsonReply.value("Result", "") == "Ok" ? true : false);
        }
    }
    return rst;
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

    int timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
    if (timer_fd == -1) {
        ServerLog->Write("[Manager] timerfd create", LOGLEVEL_ERROR);
        close(manager_fd);
        close(signal_fd);
        return 1;
    }

    itimerspec its{};
    its.it_value.tv_sec = JSON(Configuration["Configuration"]["Status Updater"]["Interval"].get<uint32_t>(), 15);
    its.it_interval.tv_sec = JSON(Configuration["Configuration"]["Status Updater"]["Interval"].get<uint32_t>(), 15);
    if (timerfd_settime(timer_fd, 0, &its, nullptr) < 0) {
        ServerLog->Write("[Manager] timerfd_settime", LOGLEVEL_ERROR);
        close(timer_fd);
        close(manager_fd);
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
        const std::string iface = JSON<string>(Configuration["Configuration"]["Bind"], "");
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
        ServerLog->Write("Bind failed - chech if TCP port " + String(Configuration["Configuration"]["Port"].get<uint16_t>()) + " is already in use", LOGLEVEL_ERROR);
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
    UpdateStatus(running);

    while (running) {
        struct pollfd pfds[3];
        pfds[0].fd = signal_fd; pfds[0].events = POLLIN;
        pfds[1].fd = manager_fd; pfds[1].events = POLLIN;
        pfds[2].fd = timer_fd; pfds[2].events = POLLIN;

        int pr = poll(pfds, 3, -1);
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

                // fprintf(stdout, "\r\n\r\n%s\r\n\r\n", incoming.c_str());

                client->IncomingBuffer(incoming);

                if (JSON<string>(client->IncomingJSON().value("Provider", "")) == Version.ProductName) {
                    const string &Command = JSON<string>(client->IncomingJSON().value("Command", ""));

                    json JsonReply;
                    bool replied = false;

                    if (Command == "CheckOnline") {
                        JsonReply = {
                            {"Provider", Version.ProductName},
                            {"Result", "Yes"},
                            {"Timestamp", CurrentDateTime()}
                        };

                        client->OutgoingBuffer(JsonReply);
                        replied = client->Reply();
                    }

                    if (Command == "ReloadConfig") {
                        ReadConfiguration();

                        JsonReply = {
                            {"Provider", Version.ProductName},
                            {"Result", "Ok"},
                            {"Timestamp", CurrentDateTime()}
                        };

                        client->OutgoingBuffer(JsonReply);
                        replied = client->Reply();
                    }

                    if (Command == "Restart") {
                        if (client->IncomingJSON().value("Parameter", "") == "ACK") {
                            ServerLog->Write("Device [" + client->IncomingJSON().value("Hostname", "") + "] sent ACK to restart", LOGLEVEL_INFO);
                        }
                    }

                    if (Command == "Discover") {
                        const json &Parameter = client->IncomingJSON()["Parameter"];
                        string r = Parameter.dump(-1);

                        if (Configuration["Managed Devices"].contains(Parameter["MAC Address"])) {
                            Configuration["Managed Devices"][Parameter["MAC Address"]] = Parameter;
                            Configuration["Managed Devices"][Parameter["MAC Address"]]["Last Update"] = CurrentDateTime();
                            Configuration["Managed Devices"][Parameter["MAC Address"]].erase("MAC Address");
                            Configuration["Managed Devices"][Parameter["MAC Address"]].erase("Server ID");

                            ServerLog->Write(Parameter["Hostname"].get<string>() + " information saved on Managed Devices section", LOGLEVEL_INFO);
                        } else {
                            Configuration["Unmanaged Devices"][Parameter["MAC Address"]] = Parameter;
                            Configuration["Unmanaged Devices"][Parameter["MAC Address"]]["Last Update"] = CurrentDateTime();
                            Configuration["Unmanaged Devices"][Parameter["MAC Address"]].erase("MAC Address");
                            Configuration["Unmanaged Devices"][Parameter["MAC Address"]].erase("Server ID");

                            ServerLog->Write(Parameter["Hostname"].get<string>() + " information saved on Unmanaged Devices section", LOGLEVEL_INFO);
                        }

                        SaveConfiguration();

                        JsonReply = {
                            {"Provider", Version.ProductName},
                            {"Result", "Ok"},
                            {"Timestamp", CurrentDateTime()}
                        };

                        client->OutgoingBuffer(JsonReply);
                        replied = client->Reply();
                    }

                    if (Command == "Pull") {
                        const json &Parameter = client->IncomingJSON()["Parameter"];
                        if (SaveDeviceConfiguration(Parameter)) {
                            JsonReply = {
                                {"Provider", Version.ProductName},
                                {"Result", "Ok"},
                                {"Timestamp", CurrentDateTime()}
                            };

                            ServerLog->Write("Configuration device " + Parameter["Network"]["Hostname"].get<String>() + " saved successfuly", LOGLEVEL_INFO);
                        } else {
                                JsonReply = {
                                {"Provider", Version.ProductName},
                                {"Result", "Fail"},
                                {"Timestamp", CurrentDateTime()}
                            };

                            ServerLog->Write("Failed saving device " + Parameter["Network"]["Hostname"].get<String>() + " configuration", LOGLEVEL_ERROR);
                        }

                        client->OutgoingBuffer(JsonReply);
                        replied = client->Reply();
                    }

                    if (replied) ServerLog->Write("Request [" + Command + "] replied to " + client->IPAddress(), LOGLEVEL_INFO);
                } else {
                    ServerLog->Write("Invalid Protocol [" + client->IncomingBuffer() + "]", LOGLEVEL_ERROR);
                }

                close(client_fd);
            } else {
                ServerLog->Write("[Manager] accept", LOGLEVEL_ERROR);
            }
        }

        if (pfds[2].revents & POLLIN) {
            uint64_t expirations = 0;
            ssize_t n = read(timer_fd, &expirations, sizeof(expirations));
            if (n != sizeof(expirations)) {
                ServerLog->Write("[Manager] read(timerfd) short read", LOGLEVEL_ERROR);
            } else {
                if (JSON<bool>(Configuration["Configuration"]["Status Updater"]["Enabled"], false)) {
                    UpdateStatus(running);
                }
            }
        }
    }

    close(manager_fd);
    close(signal_fd);
    close(timer_fd);

    UpdateStatus(running);

    ServerLog->Write("Orchestrator Manager finished", LOGLEVEL_INFO);
    return 0;
}

bool Orchestrator::SendToDevice(const std::string& destination, const json& payload) {
    if (payload.empty()) return false;
    string dumped = payload.dump(-1);

    int socket_fd, broadcast = 1;
    struct sockaddr_in broadcast_addr;

    socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd < 0) return false;

    if (mBindAddr.s_addr == 0) return false;
    (void)setsockopt(socket_fd, SOL_SOCKET, SO_BINDTODEVICE, Configuration["Configuration"]["Bind"].get<string>().c_str(), (socklen_t)Configuration["Configuration"]["Bind"].get<string>().size());

    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_port = htons(0);
    local.sin_addr = mBindAddr;
    (void)bind(socket_fd, (sockaddr*)&local, sizeof(local));

    if (setsockopt(socket_fd, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast)) < 0) {
        close(socket_fd);
        return false;
    }

    memset(&broadcast_addr, 0, sizeof(broadcast_addr));
    broadcast_addr.sin_family = AF_INET;
    broadcast_addr.sin_addr.s_addr = inet_addr(destination.c_str());
    broadcast_addr.sin_port = htons(Configuration["Configuration"]["Port"].get<uint16_t>());

    if (sendto(socket_fd, dumped.c_str(), dumped.length(), 0, (struct sockaddr *)&broadcast_addr, sizeof(broadcast_addr)) < 0) {
        close(socket_fd);
        return false;
    }

    close(socket_fd);
    return true;
}

bool Orchestrator::Discovery(const String &target) {
    json JsonCommand = {
        {"Provider", "Orchestrator"},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<String>()},
        {"Command", "Discover"},
        {"Parameter", "All"},
    };

    return SendToDevice(target, JsonCommand);
}

const json Orchestrator::getDevice(const String &target) {
    enum class Mode { ByIP, ByMAC, ByHostname };
    Mode mode = (target.find('.') != string::npos) ? Mode::ByIP : (target.find(':') != string::npos) ? Mode::ByMAC : Mode::ByHostname;

    const vector<string> sections = { "Managed Devices", "Unmanaged Devices" };

    for (const auto &section : sections) {
        if (!Configuration.contains(section) || !Configuration[section].is_object()) continue;

        for (auto it = Configuration[section].begin(); it != Configuration[section].end(); ++it) {
            const string mac_key = it.key();
            const json& obj = it.value();

            bool match = false;
            switch (mode) {
                case Mode::ByIP:
                    if (obj.contains("IP Address") && obj["IP Address"].is_string())
                        match = (obj["IP Address"].get<string>() == target);
                    break;

                case Mode::ByMAC: {
                    if (mac_key == target) {
                        match = true;
                    } else if (obj.contains("MAC Address") && obj["MAC Address"].is_string()) {
                        match = (obj["MAC Address"].get<string>() == target);
                    }
                    break;
                }

                case Mode::ByHostname:
                    if (obj.contains("Hostname") && obj["Hostname"].is_string())
                        match = (obj["Hostname"].get<string>() == target);
                    break;
            }

            if (match) {
                json device = obj;
                device["Where"] = section;
                if (!device.contains("MAC Address") || !device["MAC Address"].is_string()) device["MAC Address"] = mac_key;
                return json{{mac_key, device}};
            }
        }
    }

    return json::object();
}

bool Orchestrator::Restart(const String &target) {
    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();

    nlohmann::json JsonCommand = {
        {"Provider", "Orchestrator"},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<std::string>()},
        {"Command", "Restart"},
        {"Parameter", ""}
    };

    auto send_to_group = [&](const char* section, size_t& ok, size_t& fail, size_t& skipped) -> bool {
        if (!Configuration.contains(section) || Configuration[section].empty()) {
            ServerLog->Write(std::string("[Restart] No devices under section: ") + section, LOGLEVEL_WARNING);
            return false;
        }

        for (auto &kv : Configuration[section].items()) {
            const std::string mac = kv.key();
            const nlohmann::json &dev = kv.value();

            if (!dev.contains("IP Address") || dev["IP Address"].is_null()) {
                ServerLog->Write("[Restart] " + mac + " has no IP Address — ignored", LOGLEVEL_WARNING);
                ++skipped;
                continue;
            }

            const std::string ip = dev["IP Address"].get<std::string>();
            ServerLog->Write("[Restart] " + mac + " - " + ip, LOGLEVEL_INFO);

            const bool sent = SendToDevice(ip, JsonCommand);
            if (sent) ++ok; else ++fail;
        }
        return true;
    };

    if (target.Equals("all", true) || target.Equals("managed", true) || target.Equals("unmanaged", true)) {
        size_t ok = 0, fail = 0, skipped = 0;
        bool any_section = false;

        if (target.Equals("all", true) || target.Equals("managed", true)) {
            any_section |= send_to_group("Managed Devices", ok, fail, skipped);
        }
        if (target.Equals("all", true) || target.Equals("unmanaged", true)) {
            any_section |= send_to_group("Unmanaged Devices", ok, fail, skipped);
        }

        if (!any_section) {
            ServerLog->Write("[Restart] No devices found for requested group(s).", LOGLEVEL_WARNING);
            return false;
        }

        ServerLog->Write("[Restart] Multicast finished: Sent = " + std::to_string(ok) + ", Failed = " + std::to_string(fail) + ", Ignored = " + std::to_string(skipped), fail ? LOGLEVEL_WARNING : LOGLEVEL_INFO);

        return ok > 0;
    }

    json device = getDevice(target);
    if (device.empty()) {
        ServerLog->Write("[Restart] Target device not found: " + string(target.c_str()), LOGLEVEL_WARNING);
        return false;
    }

    auto it = device.begin();
    const string ip = it.value().at("IP Address").get<string>();
    ServerLog->Write("[Restart] " + it.key() + " - " + ip, LOGLEVEL_INFO);

    return SendToDevice(ip, JsonCommand);
}

bool Orchestrator::Refresh(const String &target) {
    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();

    nlohmann::json JsonCommand = {
        {"Provider", "Orchestrator"},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<std::string>()},
        {"Command", "Refresh"},
        {"Parameter", ""}
    };

    if (target.Equals("all", true) || target.Equals("managed", true)) {
        if (!Configuration.contains("Managed Devices") || Configuration["Managed Devices"].empty()) {
            ServerLog->Write("[Refresh] No devices managed", LOGLEVEL_WARNING);
            return false;
        }

        size_t ok = 0, fail = 0, skipped = 0;

        for (auto &kv : Configuration["Managed Devices"].items()) {
            const std::string mac = kv.key();
            const nlohmann::json &dev = kv.value();

            if (!dev.contains("IP Address") || dev["IP Address"].is_null()) {
                ServerLog->Write("[Refresh] " + mac + " no IP Address found", LOGLEVEL_WARNING);
                ++skipped;
                continue;
            }

            const std::string ip = dev["IP Address"].get<std::string>();
            ServerLog->Write("[Refresh] " + mac + " - " + ip, LOGLEVEL_INFO);

            const bool sent = SendToDevice(ip, JsonCommand);
            if (sent) ++ok; else ++fail;
        }

        ServerLog->Write("[Refresh] Multicast finished: Sent = " + std::to_string(ok) + ", Failed = " + std::to_string(fail) + ", Ignored = " + std::to_string(skipped), fail ? LOGLEVEL_WARNING : LOGLEVEL_INFO);

        return ok > 0;
    }

    nlohmann::json device = getDevice(target);
    if (device.empty()) {
        ServerLog->Write("[Refresh] Target device not found: " + target, LOGLEVEL_WARNING);
        return false;
    }

    auto it = device.begin();
    const std::string ip = it.value().at("IP Address").get<std::string>();
    ServerLog->Write("[Refresh] " + it.key() + " - " + ip, LOGLEVEL_INFO);

    return SendToDevice(ip, JsonCommand);
}

bool Orchestrator::Pull(const String &target) {
    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();

    nlohmann::json JsonCommand = {
        {"Provider", "Orchestrator"},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<String>()},
        {"Command", "Pull"},
        {"Parameter", ""}
    };

    if (target.Equals("all", true) || target.Equals("managed", true)) {
        if (!Configuration.contains("Managed Devices") || Configuration["Managed Devices"].empty()) {
            ServerLog->Write("[Refresh] No devices managed", LOGLEVEL_WARNING);
            return false;
        }

        size_t ok = 0, fail = 0, skipped = 0;

        for (auto &kv : Configuration["Managed Devices"].items()) {
            const std::string mac = kv.key();
            const nlohmann::json &dev = kv.value();

            if (!dev.contains("IP Address") || dev["IP Address"].is_null()) {
                ServerLog->Write("[Pull] " + mac + " no IP Address found", LOGLEVEL_WARNING);
                ++skipped;
                continue;
            }

            const std::string ip = dev["IP Address"].get<std::string>();
            ServerLog->Write("[Pull] " + mac + " - " + ip, LOGLEVEL_INFO);

            const bool sent = SendToDevice(ip, JsonCommand);
            if (sent) ++ok; else ++fail;
        }

        ServerLog->Write("[Pull] Multicast finished: Sent = " + std::to_string(ok) + ", Failed = " + std::to_string(fail) + ", Ignored = " + std::to_string(skipped), fail ? LOGLEVEL_WARNING : LOGLEVEL_INFO);

        return ok > 0;
    }

    nlohmann::json device = getDevice(target);
    if (device.empty()) {
        ServerLog->Write("[Pull] Target device not found: " + target, LOGLEVEL_WARNING);
        return false;
    }

    auto it = device.begin();
    const std::string ip = it.value().at("IP Address").get<String>();
    ServerLog->Write("[Pull] " + it.key() + " - " + ip, LOGLEVEL_INFO);

    return SendToDevice(ip, JsonCommand);
}
