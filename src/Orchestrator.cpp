#include "../include/Orchestrator.h"


bool OrchestratorClient::Reply() {
    if (mID <= 0) return false;
    if (mOutgoingJSON.empty()) return true;

    const std::string replymessage = mOutgoingJSON.dump(-1);
    const char* data = replymessage.data();
    size_t total_sent = 0;
    size_t remaining  = replymessage.size();

    constexpr int SEND_TIMEOUT_MS = 3000;

    while (remaining > 0) {
        ssize_t n = ::send(mID, data + total_sent, remaining, MSG_NOSIGNAL);
        if (n > 0) {
            total_sent += static_cast<size_t>(n);
            remaining  -= static_cast<size_t>(n);
            continue;
        }
        if (n == -1) {
            if (errno == EINTR) continue;

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd pfd{ mID, POLLOUT, 0 };
                int pr = ::poll(&pfd, 1, SEND_TIMEOUT_MS);
                if (pr > 0) continue;
                // timeout esperando liberar para enviar
                return false;
            }
            // erro definitivo (EPIPE, etc.)
            return false;
        }
    }

    // **Sinaliza EOF na direção de escrita** para o cliente detectar fim da resposta.
    if (::shutdown(mID, SHUT_WR) == -1) {
        // opcional: log errno (E.g., perror("shutdown"));
        return false;
    }

    // Se você NÃO precisa manter a conexão para mais nada, feche tudo:
    // ::close(mID);
    // mID = -1;

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

bool Orchestrator::Update(const String &target) {
        const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();

    nlohmann::json JsonCommand = {
        {"Provider", "Orchestrator"},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<std::string>()},
        {"Command", "Update"},
        {"Parameter", ""}
    };

    auto send_to_group = [&](const char* section, size_t& ok, size_t& fail, size_t& skipped) -> bool {
        if (!Configuration.contains(section) || Configuration[section].empty()) {
            ServerLog->Write(std::string("[Update] No devices under section: ") + section, LOGLEVEL_WARNING);
            return false;
        }

        for (auto &kv : Configuration[section].items()) {
            const std::string mac = kv.key();
            const nlohmann::json &dev = kv.value();

            if (!dev.contains("IP Address") || dev["IP Address"].is_null()) {
                ServerLog->Write("[Update] " + mac + " has no IP Address — ignored", LOGLEVEL_WARNING);
                ++skipped;
                continue;
            }

            const std::string ip = dev["IP Address"].get<std::string>();
            ServerLog->Write("[Update] " + mac + " - " + ip, LOGLEVEL_INFO);

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
            ServerLog->Write("[Update] No devices found for requested group(s).", LOGLEVEL_WARNING);
            return false;
        }

        ServerLog->Write("[Update] Multicast finished: Sent = " + std::to_string(ok) + ", Failed = " + std::to_string(fail) + ", Ignored = " + std::to_string(skipped), fail ? LOGLEVEL_WARNING : LOGLEVEL_INFO);

        return ok > 0;
    }

    json device = getDevice(target);
    if (device.empty()) {
        ServerLog->Write("[Update] Target device not found: " + string(target.c_str()), LOGLEVEL_WARNING);
        return false;
    }

    auto it = device.begin();
    const string ip = it.value().at("IP Address").get<string>();
    ServerLog->Write("[Update] " + it.key() + " - " + ip, LOGLEVEL_INFO);

    return SendToDevice(ip, JsonCommand);
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

const json Orchestrator::ReadDeviceConfiguration(const String &target) {
    json device = getDevice(target);

    if (device.empty()) return json::object();

    auto it = device.begin();
    std::string mac = it.key();
    mac.erase(std::remove(mac.begin(), mac.end(), ':'), mac.end());

    std::string config_file = "./config/" + mac + ".json";
    
    std::ifstream ifs(config_file);
    if (!ifs.is_open()) return json::object();

    std::stringstream buffer;
    buffer << ifs.rdbuf();
    std::string config_json_pretty = buffer.str();
    ifs.close();

    try {
        return json::parse(config_json_pretty);
    } catch (const std::exception& e) {
        return json::object();
    }

    return json::object();
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

// Helper: converte in_addr -> string
static inline std::string to_ip(const in_addr &addr) {
    char buf[INET_ADDRSTRLEN]{};
    inet_ntop(AF_INET, &addr, buf, sizeof(buf));
    return std::string(buf);
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

    // TCP server socket
    int manager_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (manager_fd == -1) {
        ServerLog->Write("[Manager] Socket", LOGLEVEL_ERROR);
        close(signal_fd);
        return 1;
    }

    // ---- UDP socket para Discover ----
    int udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_fd == -1) {
        ServerLog->Write("[Manager] UDP socket", LOGLEVEL_ERROR);
        close(manager_fd);
        close(signal_fd);
        return 1;
    }

    int one = 1;
    setsockopt(manager_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    setsockopt(udp_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(udp_fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
    setsockopt(udp_fd, SOL_SOCKET, SO_BROADCAST, &one, sizeof(one));
    setsockopt(udp_fd, IPPROTO_IP, IP_PKTINFO, &one, sizeof(one));

    // timerfd para status
    int timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
    if (timer_fd == -1) {
        ServerLog->Write("[Manager] timerfd create", LOGLEVEL_ERROR);
        close(udp_fd);
        close(manager_fd);
        close(signal_fd);
        return 1;
    }

    itimerspec its{};
    its.it_value.tv_sec = JSON(Configuration["Configuration"]["Status Updater"]["Interval"].get<uint32_t>(), 15);
    its.it_interval.tv_sec = JSON(Configuration["Configuration"]["Status Updater"]["Interval"].get<uint32_t>(), 15);
    its.it_value.tv_nsec = 0;
    its.it_interval.tv_nsec = 0;
    if (timerfd_settime(timer_fd, 0, &its, nullptr) < 0) {
        ServerLog->Write("[Manager] timerfd_settime", LOGLEVEL_ERROR);
        close(timer_fd);
        close(udp_fd);
        close(manager_fd);
        close(signal_fd);
        return 1;
    }

    // Opcional: travar TCP em interface específica se configurado
    if (mBindAddr.s_addr != 0) {
        const std::string iface = JSON<string>(Configuration["Configuration"]["Bind"], "");
        if (!iface.empty()) {
            if (setsockopt(manager_fd, SOL_SOCKET, SO_BINDTODEVICE, iface.c_str(), (socklen_t)iface.size()) < 0) {
                ServerLog->Write("[Manager] setsockopt(SO_BINDTODEVICE) TCP falhou; prosseguindo", LOGLEVEL_WARNING);
            }
            // IMPORTANTE: não aplicar SO_BINDTODEVICE no udp_fd para não perder broadcasts
        }
    }

    // Bind TCP
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(Configuration["Configuration"]["Port"].get<uint16_t>());
    address.sin_addr = (mBindAddr.s_addr != 0) ? mBindAddr : in_addr{ htonl(INADDR_ANY) };

    if (bind(manager_fd, (sockaddr*)&address, sizeof(address)) < 0) {
        ServerLog->Write("Bind failed - check if TCP port " + String(Configuration["Configuration"]["Port"].get<uint16_t>()) + " is already in use", LOGLEVEL_ERROR);
        close(timer_fd);
        close(udp_fd);
        close(manager_fd);
        close(signal_fd);
        return 1;
    }

    if (listen(manager_fd, 10) < 0) {
        ServerLog->Write("[Manager] Listen", LOGLEVEL_ERROR);
        close(timer_fd);
        close(udp_fd);
        close(manager_fd);
        close(signal_fd);
        return 1;
    }

    // Bind UDP (usar SEMPRE INADDR_ANY para receber broadcast)
    sockaddr_in uaddr{};
    uaddr.sin_family = AF_INET;
    uaddr.sin_port   = htons(Configuration["Configuration"]["Port"].get<uint16_t>());
    uaddr.sin_addr   = in_addr{ htonl(INADDR_ANY) };

    if (bind(udp_fd, (sockaddr*)&uaddr, sizeof(uaddr)) < 0) {
        ServerLog->Write("Bind failed - check if UDP port " + String(Configuration["Configuration"]["Port"].get<uint16_t>()) + " is already in use", LOGLEVEL_ERROR);
        close(timer_fd);
        close(udp_fd);
        close(manager_fd);
        close(signal_fd);
        return 1;
    }

    {
        char iptcp[INET_ADDRSTRLEN]; inet_ntop(AF_INET, &address.sin_addr, iptcp, sizeof(iptcp));
        ServerLog->Write(String("TCP bound at ") + iptcp + ":" + String(ntohs(address.sin_port)), LOGLEVEL_INFO);
        char ipudp[INET_ADDRSTRLEN]; inet_ntop(AF_INET, &uaddr.sin_addr, ipudp, sizeof(ipudp));
        ServerLog->Write(String("UDP bound at ") + ipudp + ":" + String(ntohs(uaddr.sin_port)), LOGLEVEL_INFO);
    }

    const size_t buf_sz = Configuration["Configuration"]["Buffer Size"].get<size_t>();
    ServerLog->Write("Orchestrator Manager is running (pid " + String(getpid()) + ")", LOGLEVEL_INFO);

    bool running = true;
    UpdateStatus(running);

    while (running) {
        struct pollfd pfds[4];
        pfds[0].fd = signal_fd; pfds[0].events = POLLIN;
        pfds[1].fd = manager_fd; pfds[1].events = POLLIN;
        pfds[2].fd = timer_fd; pfds[2].events = POLLIN;
        pfds[3].fd = udp_fd; pfds[3].events = POLLIN;

        int pr = poll(pfds, 4, -1);
        if (pr < 0) {
            if (errno == EINTR) continue;
            ServerLog->Write("[Manager] poll", LOGLEVEL_ERROR);
            break;
        }

        auto had_err = [&](int i) {
            if (pfds[i].revents & (POLLERR | POLLHUP | POLLNVAL)) {
                ServerLog->Write(String("[Manager] poll fd ") + String(pfds[i].fd) + " error flags=" + String(pfds[i].revents), LOGLEVEL_WARNING);
                return true;
            }
            return false;
        };
        had_err(0); had_err(1); had_err(2); had_err(3);

        // sinais
        if (pfds[0].revents & POLLIN) {
            signalfd_siginfo si{};
            ssize_t n = read(signal_fd, &si, sizeof(si));
            if (n == sizeof(si)) {
                switch (si.ssi_signo) {
                    case SIGINT:  ServerLog->Write("SIGINT (Ctrl+C)", LOGLEVEL_INFO); running = false; break;
                    case SIGTERM: ServerLog->Write("SIGTERM", LOGLEVEL_INFO);         running = false; break;
                    case SIGHUP:  ServerLog->Write("Reload configuration file " + mConfigFile, LOGLEVEL_INFO); ReadConfiguration(); break;
                    case SIGUSR1: ServerLog->Write("SIGUSR1", LOGLEVEL_INFO); break;
                    case SIGUSR2: ServerLog->Write("SIGUSR2", LOGLEVEL_INFO); break;
                    case SIGPIPE: ServerLog->Write("SIGPIPE", LOGLEVEL_INFO); break;
                    default:      ServerLog->Write("Signal not addressed: " + String(si.ssi_signo), LOGLEVEL_INFO); break;
                }
            } else {
                ServerLog->Write("[Manager] read(signalfd) short read", LOGLEVEL_ERROR);
            }
        }

        // conexões TCP
        if (pfds[1].revents & POLLIN) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(manager_fd, (sockaddr*)&client_addr, &client_len);
            if (client_fd < 0) {
                ServerLog->Write("[Manager] accept", LOGLEVEL_ERROR);
                continue;
            }

            // delimitação por idle (~1.5s)
            timeval rcv_to{}; rcv_to.tv_sec = 1; rcv_to.tv_usec = 500000;
            setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &rcv_to, sizeof(rcv_to));

            std::string incoming;
            std::vector<char> buffer(buf_sz);
            for (;;) {
                ssize_t r = ::recv(client_fd, buffer.data(), buffer.size(), 0);
                if (r > 0) { incoming.append(buffer.data(), (size_t)r); continue; }
                if (r == 0) break;
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                ServerLog->Write("[Manager] recv error", LOGLEVEL_ERROR);
                break;
            }

            OrchestratorClient *client = new OrchestratorClient(client_fd, client_addr);
            client->IncomingBuffer(incoming);

            if (JSON<string>(client->IncomingJSON().value("Provider", "")) == Version.ProductName) {
                const string &Command = JSON<string>(client->IncomingJSON().value("Command", ""));

                json JsonReply;
                bool replied = false;

                if (Command == "CheckOnline") replied = handle_CheckOnline(client);
                if (Command == "ReloadConfig") replied = handle_ReloadConfig(client);
                if (Command == "Restart") replied = handle_Restart(client);
                if (Command == "Update") replied = handle_Update(client);
                if (Command == "Discover") replied = handle_Discover(client);
                if (Command == "Pull") replied = handle_Pull(client);
                if (Command == "Push") replied = handle_Push(client);
                if (Command == "GetLog") replied = handle_GetLog(client);

                if (replied) {
                    ServerLog->Write("Request [" + Command + "] replied to " + client->IPAddress(), LOGLEVEL_INFO);
                } else {
                    ::shutdown(client_fd, SHUT_WR);
                }

            } else {
                ServerLog->Write("Invalid Protocol [" + client->IncomingBuffer() + "]", LOGLEVEL_ERROR);
            }

            close(client_fd);
        }

        // timer
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

        // ---- UDP Discover ----
        if (pfds[3].revents & POLLIN) {
            // helpers locais para resolver IPv4 da interface
            auto iface_ipv4 = [](const char* ifname, in_addr& out)->bool {
                struct ifaddrs* ifaddr = nullptr;
                if (getifaddrs(&ifaddr) == -1) return false;
                bool ok = false;
                for (auto* ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
                    if (!ifa->ifa_addr) continue;
                    if (ifa->ifa_addr->sa_family != AF_INET) continue;
                    if (strcmp(ifa->ifa_name, ifname) != 0) continue;
                    // Ignora loopback
                    if (ifa->ifa_flags & IFF_LOOPBACK) continue;
                    out = ((sockaddr_in*)ifa->ifa_addr)->sin_addr;
                    ok = true;
                    break;
                }
                freeifaddrs(ifaddr);
                return ok;
            };

            auto first_nonloop_ipv4 = [](in_addr& out)->bool {
                struct ifaddrs* ifaddr = nullptr;
                if (getifaddrs(&ifaddr) == -1) return false;
                bool ok = false;
                for (auto* ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
                    if (!ifa->ifa_addr) continue;
                    if (ifa->ifa_addr->sa_family != AF_INET) continue;
                    if (ifa->ifa_flags & IFF_LOOPBACK) continue;
                    out = ((sockaddr_in*)ifa->ifa_addr)->sin_addr;
                    ok = true;
                    break;
                }
                freeifaddrs(ifaddr);
                return ok;
            };

            // usar recvmsg para obter a ifindex de chegada
            char inbuf[2048];
            struct iovec iov{}; iov.iov_base = inbuf; iov.iov_len = sizeof(inbuf);

            char cmsgbuf[CMSG_SPACE(sizeof(in_pktinfo))];
            struct msghdr msg{};
            sockaddr_in peer{};
            msg.msg_name       = &peer;
            msg.msg_namelen    = sizeof(peer);
            msg.msg_iov        = &iov;
            msg.msg_iovlen     = 1;
            msg.msg_control    = cmsgbuf;
            msg.msg_controllen = sizeof(cmsgbuf);

            ssize_t r = recvmsg(udp_fd, &msg, 0);
            if (r > 0) {
                std::string payload(inbuf, inbuf + r);

                nlohmann::json j;
                bool ok = false;
                try { j = nlohmann::json::parse(payload); ok = true; } catch (...) {}

                if (ok && j.is_object() && j.value("Orchestrator", std::string()) == "Discover") {
                    // 1) Tentamos usar a interface do TCP listener (Bind)
                    in_addr local_ip{}; local_ip.s_addr = 0;
                    std::string bind_iface = JSON<string>(Configuration["Configuration"]["Bind"], "");

                    if (!bind_iface.empty()) {
                        // SO_BINDTODEVICE no TCP -> responder com o IPv4 dessa iface
                        if (!iface_ipv4(bind_iface.c_str(), local_ip)) {
                            // fallback: se não achar IP da iface, usa o que foi bindado no TCP (se específico)
                            if (address.sin_addr.s_addr != htonl(INADDR_ANY))
                                local_ip = address.sin_addr;
                        }
                    }

                    // 2) Se ainda não temos IP, tenta pela ifindex do pacote recebido
                    if (local_ip.s_addr == 0) {
                        in_pktinfo* pi = nullptr;
                        for (struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
                            cmsg != nullptr;
                            cmsg = CMSG_NXTHDR(&msg, cmsg)) {
                            if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_PKTINFO) {
                                pi = (in_pktinfo*)CMSG_DATA(cmsg);
                                break;
                            }
                        }
                        if (pi) {
                            char ifname[IFNAMSIZ] = {0};
                            if_indextoname(pi->ipi_ifindex, ifname);
                            if (ifname[0] != '\0') {
                                (void)iface_ipv4(ifname, local_ip); // se falhar, ainda tentamos fallback abaixo
                            }
                        }
                    }

                    // 3) Fallbacks finais
                    if (local_ip.s_addr == 0 && mBindAddr.s_addr != 0 && mBindAddr.s_addr != htonl(INADDR_ANY)) {
                        local_ip = mBindAddr;
                    }
                    if (local_ip.s_addr == 0 && address.sin_addr.s_addr != htonl(INADDR_ANY)) {
                        local_ip = address.sin_addr;
                    }
                    if (local_ip.s_addr == 0) {
                        // último recurso: primeiro IPv4 não-loopback do host
                        (void)first_nonloop_ipv4(local_ip);
                    }

                    nlohmann::json reply = {
                        {"Orchestrator", {
                            {"IP Address", to_ip(local_ip)},
                            {"Server ID",  Configuration["Configuration"]["Server ID"].get<std::string>()}
                        }}
                    };

                    auto out = reply.dump();
                    if (sendto(udp_fd, out.data(), out.size(), 0, (sockaddr*)&peer, sizeof(peer)) < 0) {
                        ServerLog->Write("[UDP] sendto failed", LOGLEVEL_WARNING);
                    } else {
                        ServerLog->Write("UDP Discover replied to " + String(to_ip(peer.sin_addr)), LOGLEVEL_INFO);
                    }
                } else {
                    // opcional: log para depuração
                    // ServerLog->Write("UDP payload ignored: " + String(payload.c_str()), LOGLEVEL_DEBUG);
                }
            } else if (r < 0 && errno != EAGAIN && errno != EINTR) {
                ServerLog->Write("[UDP] recvmsg error", LOGLEVEL_WARNING);
            }
        }

    } // while

    close(manager_fd);
    close(signal_fd);
    close(timer_fd);
    close(udp_fd);

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
            ServerLog->Write("[Pull] No devices managed", LOGLEVEL_WARNING);
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

bool Orchestrator::GetLog(const String &target) {
    // TODO
    return true;
}

bool Orchestrator::Push(const String &target) {
    const uint16_t port = Configuration["Configuration"]["Port"].get<uint16_t>();

    nlohmann::json JsonCommand = {
        {"Provider", "Orchestrator"},
        {"Server ID", Configuration["Configuration"]["Server ID"].get<String>()},
        {"Command", "Push"},
        {"Parameter", ""}
    };

    if (target.Equals("all", true) || target.Equals("managed", true)) {
        if (!Configuration.contains("Managed Devices") || Configuration["Managed Devices"].empty()) {
            ServerLog->Write("[Push] No devices managed", LOGLEVEL_WARNING);
            return false;
        }

        size_t ok = 0, fail = 0, skipped = 0;

        for (auto &kv : Configuration["Managed Devices"].items()) {
            const std::string mac = kv.key();
            const nlohmann::json &dev = kv.value();

            if (!dev.contains("IP Address") || dev["IP Address"].is_null()) {
                ServerLog->Write("[Push] " + mac + " no IP Address found", LOGLEVEL_WARNING);
                ++skipped;
                continue;
            }

            const std::string ip = dev["IP Address"].get<std::string>();
            ServerLog->Write("[Push] " + mac + " - " + ip, LOGLEVEL_INFO);

            const bool sent = SendToDevice(ip, JsonCommand);
            if (sent) ++ok; else ++fail;
        }

        ServerLog->Write("[Push] Multicast finished: Sent = " + std::to_string(ok) + ", Failed = " + std::to_string(fail) + ", Ignored = " + std::to_string(skipped), fail ? LOGLEVEL_WARNING : LOGLEVEL_INFO);

        return ok > 0;
    }

    nlohmann::json device = getDevice(target);
    if (device.empty()) {
        ServerLog->Write("[Push] Target device not found: " + target, LOGLEVEL_WARNING);
        return false;
    }

    auto it = device.begin();
    const std::string ip = it.value().at("IP Address").get<String>();
    ServerLog->Write("[Push] " + it.key() + " - " + ip, LOGLEVEL_INFO);

    return SendToDevice(ip, JsonCommand);
}






// Private methods
bool Orchestrator::replyClient(OrchestratorClient* &client, const json &result) {
    json payload = {{"Provider", Version.ProductName}, {"Result", result}, {"Timestamp", CurrentDateTime()}};
    client->OutgoingBuffer(payload);
    return client->Reply();
}

bool Orchestrator::handle_CheckOnline(OrchestratorClient* &client) {
    return replyClient(client, "Yes");
}

bool Orchestrator::handle_ReloadConfig(OrchestratorClient* &client) {
    ReadConfiguration();
    return replyClient(client, "Ok");
}

bool Orchestrator::handle_Restart(OrchestratorClient* &client) {
    if (client->IncomingJSON().value("Parameter", "") == "ACK") {
        ServerLog->Write("Device [" + client->IncomingJSON().value("Hostname", "") + "] sent ACK to restart", LOGLEVEL_INFO);
        return true;
    }
    return false;
}

bool Orchestrator::handle_Update(OrchestratorClient* &client) {
    if (client->IncomingJSON().value("Parameter", "") == "ACK") {
        ServerLog->Write("Device [" + client->IncomingJSON().value("Hostname", "") + "] sent ACK to update", LOGLEVEL_INFO);
        return true;
    }
    return false;
}

bool Orchestrator::handle_Discover(OrchestratorClient*& client) {
    const json& Parameter = client->IncomingJSON().at("Parameter");

    const std::string mac = Parameter.at("MAC Address").get<std::string>();
    const std::string hostname = Parameter.value("Hostname", "<unknown>");

    auto& managed   = Configuration["Managed Devices"];
    auto& unmanaged = Configuration["Unmanaged Devices"];

    const bool isManaged = managed.contains(mac);
    auto& bucket = isManaged ? managed : unmanaged;

    json rec = Parameter;
    rec["Last Update"] = CurrentDateTime();
    rec.erase("MAC Address");
    rec.erase("Server ID");

    bucket[mac] = std::move(rec);

    ServerLog->Write(hostname + " information saved on " + string(isManaged ? "Managed Devices" : "Unmanaged Devices") + " section", LOGLEVEL_INFO);

    SaveConfiguration();
    return replyClient(client, "Ok");
}

bool Orchestrator::handle_Pull(OrchestratorClient*& client) {
    const json &Parameter = client->IncomingJSON()["Parameter"];
    if (SaveDeviceConfiguration(Parameter)) {
        ServerLog->Write("Configuration device " + Parameter["Network"]["Hostname"].get<String>() + " saved successfully", LOGLEVEL_INFO);
        return replyClient(client, "Ok");
    }
    ServerLog->Write("Failed saving device " + Parameter["Network"]["Hostname"].get<String>() + " configuration", LOGLEVEL_ERROR);
    return replyClient(client, "Fail");
}

bool Orchestrator::handle_Push(OrchestratorClient*& client) {
    const String &Parameter = client->IncomingJSON()["Parameter"].get<String>();
    json r = ReadDeviceConfiguration(Parameter);
    if (r.empty()) {
        ServerLog->Write("Failed reading device " + Parameter + " configuration file", LOGLEVEL_ERROR);
        return false;
    }
    return replyClient(client, r);
}

bool Orchestrator::handle_GetLog(OrchestratorClient*& client) {
    // TODO
}