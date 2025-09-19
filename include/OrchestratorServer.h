#ifndef ORCHESTRATORSERVER_H
#define ORCHESTRATORSERVER_H

#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <ifaddrs.h>
#include <iostream>
#include <net/if.h>
#include <netdb.h>
#include <nlohmann/json.hpp>
#include <poll.h>
#include <random>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <system_error>
#include <thread>
#include <unistd.h>

#include "OrchestratorClient.h"
#include "String.h"
#include "Log.h"
#include "Tools.h"

using namespace std;
using namespace Orchestrator_Log;
using namespace Tools;

using json = nlohmann::json;

extern Orchestrator_Log::Log *ServerLog;

constexpr char DEF_SERVERNAME[] = "Orchestrator";

template <typename T>
T JSON(const nlohmann::json& jsonValue, const T& defaultValue = T()) {
    try {
        if (jsonValue.is_null()) return defaultValue;
        return jsonValue.get<T>();
    } catch (const nlohmann::json::type_error&) {
        return defaultValue;
    } catch (const nlohmann::json::out_of_range&) {
        return defaultValue;
    }
}

class OrchestratorServer {
    private:
        string mConfigFile = "./Orchestrator.json";
        string mLogFile = "./Orchestrator.log";
        string mBindInterface = "";
        string mServerStartedTimestamp;

        in_addr mBindAddr{};

        bool replyClient(OrchestratorClient* &client, const json &result);
        bool handle_CheckOnline(OrchestratorClient* &client);
        bool handle_Restart(OrchestratorClient* &client);
        bool handle_Update(OrchestratorClient* &client);
        bool handle_Discover(OrchestratorClient* &client);
        bool handle_Pull(OrchestratorClient* &client);
        bool handle_Push(OrchestratorClient* &client);
        bool handle_GetLog(OrchestratorClient* &client);

        std::string generateRandomID() { std::string result; const std::string valid_characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; std::random_device rd; std::mt19937 generator(rd()); std::uniform_int_distribution<> distribution(0, valid_characters.size() - 1); for (int i = 0; i < 15; ++i) { result += valid_characters[distribution(generator)]; } return result; }
        std::string queryIPAddress(const char* mac_address);
        std::string queryMACAddress(const char* ip_address);

        bool readConfiguration();
        bool saveConfiguration() { ofstream outFile(mConfigFile.c_str()); if (!outFile.is_open()) return false; outFile << Configuration.dump(4); outFile.close(); return !outFile.fail(); }

        inline bool isManaged(const String &target) { return Configuration["Managed Devices"].contains(target); }
        const json getDevice(const String &target);

        void applyBindForUdpSocket(int sockfd);
        bool resolveInterfaceOrIp(const string& ifaceOrIp, in_addr& out);
        bool setBindInterface(const std::string& ifaceOrIp);

        void init();
    public:
        explicit OrchestratorServer(const std::string &configfile, const std::string &bindinfertace) : mConfigFile(configfile), mBindInterface(bindinfertace) { init(); }

        inline std::string ConfigFile() const { return mConfigFile; }
        inline const string &ServerStartedTimestamp() const { return mServerStartedTimestamp; }

        void UpdateStatus(bool status);
        int Manage();
        
        json Query(const std::string& orchestrator_url, uint16_t orchestrator_port, const json& payload);
        
        bool SaveDeviceConfiguration(const json &cfg);
        const json ReadDeviceConfiguration(const String &target);

        json Configuration;
};

#endif // ORCHESTRATORSERVER_H