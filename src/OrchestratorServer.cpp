#include "../include/OrchestratorServer.h"

void OrchestratorServer::init() {
    mServerStartedTimestamp = CurrentDateTime();
    if (readConfiguration()) {
        mLogFile = JSON<String>(Configuration["Configuration"]["Log File"], mLogFile);
        if (JSON<bool>(Configuration["Configuration"]["New Log"], false)) std::remove(mLogFile.c_str());
        
        String bind_if;
        if (mBindInterface.empty()) {
            bind_if = JSON<String>(Configuration["Configuration"]["Bind"], "");
        } else {
            bind_if = mBindInterface;
        }

        if (!setBindInterface(bind_if)) {
            throw std::runtime_error("Failed to bind to interface " + bind_if);
        }
    } else {    
        throw std::runtime_error("Failed to read configuration file " + mConfigFile);
    }
}

std::string OrchestratorServer::queryIPAddress(const char* mac_address) {
    if (Configuration.contains("Managed Devices") || Configuration["Managed Devices"].is_object()) {
        for (const auto& [device_id, device_info] : Configuration["Managed Devices"].items()) {
            if (device_info.value("MAC Address", "") == mac_address) {
                return device_info.value("IP Address", "IP Not Found");
            }
        }
    }
    
    return "IP Not Found";
}

std::string OrchestratorServer::queryMACAddress(const char* ip_address) {
    if (Configuration.contains("Managed Devices") || Configuration["Managed Devices"].is_object()) {
        for (const auto& [device_id, device_info] : Configuration["Managed Devices"].items()) {
            if (device_info.value("IP Address", "") == ip_address) {
                return device_info.value("MAC Address", "MAC Not Found");
            }
        }
    }
    
    return "MAC Not Found";
}

void OrchestratorServer::applyBindForUdpSocket(int sockfd) {
    if (mBindAddr.s_addr == 0) return;
    (void)setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, Configuration["Configuration"]["Bind"].get<string>().c_str(), (socklen_t)Configuration["Configuration"]["Bind"].get<string>().size());

    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_port = htons(0);
    local.sin_addr = mBindAddr;
    (void)bind(sockfd, (sockaddr*)&local, sizeof(local));
}

bool OrchestratorServer::readConfiguration() {
    bool ret = false;

    std::ifstream configFile(mConfigFile);

    if (configFile.is_open()) {
        try {
            configFile >> Configuration;
            ret = true;
        } catch (nlohmann::json::parse_error& ex) {
            throw "Parse error at byte " + to_string(ex.byte) + ":" + ex.what() + " on file " + mConfigFile;
            ret = false;
        }
    } else {
        Configuration["Configuration"] = {
            {"Bind", ""},
            {"Buffer Size", 1024},
            {"Log Endpoint", "ENDPOINT_CONSOLE, ENDPOINT_FILE, ENDPOINT_SYSLOG_LOCAL, ENDPOINT_SYSLOG_REMOTE"},
            {"Log File", "./Orchestrator.log"},
            {"New Log", true},
            {"Port", 30030},
            {"Server ID", generateRandomID()},
            {"Server Name", DEF_SERVERNAME},
            {"Syslog Port", 514},
            {"Syslog URL", ""},
            {"Timeout", 15},
            {"Token", DEF_SERVERNAME},
        };
        
        Configuration["Configuration"]["Status Updater"] = {
            {"Enabled", false},
            {"Interval", 60000},
            {"Output File", "/var/www/html/status.json"}
        };
        
        return saveConfiguration();
    }

    return ret;
}

bool OrchestratorServer::SaveDeviceConfiguration(const json &config) {
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

const json OrchestratorServer::ReadDeviceConfiguration(const String &target) {
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

bool OrchestratorServer::resolveInterfaceOrIp(const std::string& ifaceOrIp, in_addr& out) {
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

bool OrchestratorServer::setBindInterface(const std::string& ifaceOrIp) {
    if (ifaceOrIp.empty()) {
        mBindAddr = {};
        return true;
    }

    in_addr addr{};
    if (!resolveInterfaceOrIp(ifaceOrIp, addr)) return false;

    mBindAddr = addr;
    
    return true;
}

json OrchestratorServer::Query(const std::string& orchestrator_url, uint16_t orchestrator_port, const json& payload) {
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

void OrchestratorServer::UpdateStatus(bool status) {
    json JsonStatusUpdater;
    JsonStatusUpdater["Orchestrator"]["Status"] = {
        {"Version", Version.Software.Info()},
        {"Online", status},
        {"Server Started", ServerStartedTimestamp()},
        {"Last Update", CurrentDateTime()}
    };

    ofstream outFile(JSON<string>(Configuration["Configuration"]["Status Updater"]["Output File"], "./status.json").c_str());
    if (!outFile.is_open()) ServerLog->Write("Failed to open Orchestrator status update file " + JSON<string>(Configuration["Configuration"]["Status Updater"]["Output File"], "./status.json"), LOGLEVEL_ERROR);

    outFile << JsonStatusUpdater.dump(4);
    outFile.close();

    if (!outFile.fail()) ServerLog->Write("Orchestrator status file " + JSON<string>(Configuration["Configuration"]["Status Updater"]["Output File"], "./status.json") + " updated", LOGLEVEL_INFO);
}

// Helper: converte in_addr -> string
static inline std::string to_ip(const in_addr &addr) {
    char buf[INET_ADDRSTRLEN]{};
    inet_ntop(AF_INET, &addr, buf, sizeof(buf));
    return std::string(buf);
}

int OrchestratorServer::Manage() {
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

    // ---- UDP socket for Discover ----
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

    char iptcp[INET_ADDRSTRLEN]; inet_ntop(AF_INET, &address.sin_addr, iptcp, sizeof(iptcp));
    ServerLog->Write(String("TCP bound at ") + iptcp + ":" + String(ntohs(address.sin_port)), LOGLEVEL_INFO);
    char ipudp[INET_ADDRSTRLEN]; inet_ntop(AF_INET, &uaddr.sin_addr, ipudp, sizeof(ipudp));
    ServerLog->Write(String("UDP bound at ") + ipudp + ":" + String(ntohs(uaddr.sin_port)), LOGLEVEL_INFO);

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

        // Signals
        if (pfds[0].revents & POLLIN) {
            signalfd_siginfo si{};
            ssize_t n = read(signal_fd, &si, sizeof(si));
            if (n == sizeof(si)) {
                switch (si.ssi_signo) {
                    case SIGINT: ServerLog->Write("Orchestrator Server was stopped", LOGLEVEL_INFO); running = false; break;
                    case SIGTERM: ServerLog->Write("Orchestrator Server was terminated", LOGLEVEL_INFO); running = false; break;
                    case SIGHUP: ServerLog->Write("Reload configuration file " + mConfigFile, LOGLEVEL_INFO); readConfiguration(); break;
                    case SIGUSR1: ServerLog->Write("SIGUSR1", LOGLEVEL_INFO); break;
                    case SIGUSR2: ServerLog->Write("SIGUSR2", LOGLEVEL_INFO); break;
                    case SIGPIPE: ServerLog->Write("SIGPIPE", LOGLEVEL_INFO); break;
                    default: ServerLog->Write("Signal not addressed: " + String(si.ssi_signo), LOGLEVEL_INFO); break;
                }
            } else {
                ServerLog->Write("[Manager] read(signalfd) short read", LOGLEVEL_ERROR);
            }
        }

        // TCP Connections
        if (pfds[1].revents & POLLIN) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(manager_fd, (sockaddr*)&client_addr, &client_len);
            if (client_fd < 0) {
                ServerLog->Write("[Manager] accept", LOGLEVEL_ERROR);
                continue;
            }

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

            // Debug - print whatever arrives
            // std::cout << "\r\n---\r\n" << incoming << "\r\n---\r\n" << std::endl;
            // 

            if ((!client->IncomingJSON().empty()) && (JSON<string>(client->IncomingJSON().value("Provider", "")) == Version.Provider)) {
                const string &Command = JSON<string>(client->IncomingJSON().value("Command", ""));

                json JsonReply;
                bool replied = false;

                if (Command == "CheckOnline") replied = handle_CheckOnline(client);
                if (Command == "Restart") replied = handle_Restart(client);
                if (Command == "Update") replied = handle_Update(client);
                if (Command == "Discover") replied = handle_Discover(client);
                if (Command == "Pull") replied = handle_Pull(client);
                if (Command == "Push") replied = handle_Push(client);
                if (Command == "GetLog") replied = handle_GetLog(client);

                if (replied) {
                    // ServerLog->Write("Request [" + Command + "] replied to " + client->IPAddress(), LOGLEVEL_INFO);
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
            auto iface_ipv4 = [](const char* ifname, in_addr& out)->bool {
                struct ifaddrs* ifaddr = nullptr;
                if (getifaddrs(&ifaddr) == -1) return false;
                bool ok = false;
                for (auto* ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
                    if (!ifa->ifa_addr) continue;
                    if (ifa->ifa_addr->sa_family != AF_INET) continue;
                    if (strcmp(ifa->ifa_name, ifname) != 0) continue;
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
            msg.msg_name = &peer;
            msg.msg_namelen = sizeof(peer);
            msg.msg_iov = &iov;
            msg.msg_iovlen = 1;
            msg.msg_control = cmsgbuf;
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
    ServerLog->Write("Orchestrator Server finished", LOGLEVEL_INFO);
    return 0;
}

const json OrchestratorServer::getDevice(const String &target) {
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

// Private methods
bool OrchestratorServer::replyClient(OrchestratorClient* &client, const json &result) {
    json payload = {{"Provider", Version.ProductName}, {"Result", result}, {"Timestamp", CurrentDateTime()}};
    client->OutgoingBuffer(payload);
    return client->Reply();
}

bool OrchestratorServer::handle_CheckOnline(OrchestratorClient* &client) {
    return replyClient(client, "Yes");
}

bool OrchestratorServer::handle_Restart(OrchestratorClient* &client) {
    if (client->IncomingJSON().value("Parameter", "") == "ACK") {
        ServerLog->Write("Device [" + client->IncomingJSON().value("Hostname", "") + "] sent ACK to restart", LOGLEVEL_INFO);
        return true;
    }
    return false;
}

bool OrchestratorServer::handle_Update(OrchestratorClient* &client) {
    if (client->IncomingJSON().value("Parameter", "") == "ACK") {
        ServerLog->Write("Device [" + client->IncomingJSON().value("Hostname", "") + "] sent ACK to update", LOGLEVEL_INFO);
        return true;
    }
    return false;
}

bool OrchestratorServer::handle_Discover(OrchestratorClient*& client) {
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
    saveConfiguration();
    return replyClient(client, "Ok");
}

bool OrchestratorServer::handle_Pull(OrchestratorClient*& client) {
    const json &Parameter = client->IncomingJSON()["Parameter"];
    if (SaveDeviceConfiguration(Parameter)) {
        ServerLog->Write("Configuration device " + Parameter["Network"]["Hostname"].get<String>() + " saved successfully", LOGLEVEL_INFO);
        return replyClient(client, "Ok");
    }
    ServerLog->Write("Failed saving device " + Parameter["Network"]["Hostname"].get<String>() + " configuration", LOGLEVEL_ERROR);
    return replyClient(client, "Fail");
}

bool OrchestratorServer::handle_Push(OrchestratorClient*& client) {
    const String &Parameter = client->IncomingJSON()["Parameter"].get<String>();
    json r = ReadDeviceConfiguration(Parameter);
    if (r.empty()) {
        ServerLog->Write("Failed reading device " + Parameter + " configuration file", LOGLEVEL_ERROR);
        return false;
    }
    return replyClient(client, r);
}

bool OrchestratorServer::handle_GetLog(OrchestratorClient*& client) {
    const json &Parameter = client->IncomingJSON()["Parameter"];

    if (SaveDeviceLog(Parameter)) {
        ServerLog->Write("Log device " + Parameter["MAC Address"].get<String>() + " saved successfully", LOGLEVEL_INFO);
        return replyClient(client, "Ok");
    }
    ServerLog->Write("Failed saving device " + Parameter["Hostname"].get<String>() + " log", LOGLEVEL_ERROR);
    return replyClient(client, "Fail");
}

bool OrchestratorServer::SaveDeviceLog(const json &payload) {
    std::cout << payload.dump(4) << std::endl;

    // std::string mac = config["Network"]["MAC Address"].get<std::string>();
    // mac.erase(std::remove(mac.begin(), mac.end(), ':'), mac.end());

    // std::string config_file = "./log/" + mac + ".log";

    // filesystem::create_directories("./log");

    // std::ofstream outFile(config_file);
    // if (!outFile.is_open()) return false;

    // outFile << config.dump(4);
    // outFile.close();

    // return !outFile.fail();

    return true;
}