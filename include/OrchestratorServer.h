#ifndef ORCHESTRATORSERVER_H
#define ORCHESTRATORSERVER_H

#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <net/if.h>
#include <ifaddrs.h>
#include <unistd.h>
#include <thread>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <random>
#include <chrono>
#include <fstream>
#include <netdb.h>
#include <poll.h>
#include <errno.h>
#include <filesystem>
#include <system_error>
#include <csignal>
#include <nlohmann/json.hpp>

#include "../include/OrchestratorClient.h"
#include "String.h"
#include "Log.h"
#include "Tools.h"

using namespace std;
using namespace Orchestrator_Log;
using namespace Tools;

using json = nlohmann::json;

extern Orchestrator_Log::Log *ServerLog;

constexpr size_t DEF_BUFFERSIZE = 1024;
constexpr uint32_t DEF_LISTENTIMEOUT = 5;
constexpr uint16_t DEF_PORT = 30030;
constexpr char DEF_BROADCASTADDRESS[] = "255.255.255.255";
constexpr char DEF_SERVERNAME[] = "Orchestrator";

enum OrchestratorAction { ACTION_NOACTION, ACTION_DISCOVERY, ACTION_ADD, ACTION_RESTART, ACTION_RELOADCONFIG, ACTION_REMOVE, ACTION_UPDATE, ACTION_REFRESH, ACTION_LIST, ACTION_PULL, ACTION_PUSH, ACTION_MANAGE, ACTION_CHECKONLINE, ACTION_GETLOG };

enum OperationResult {
    NOTMANAGED,

    ADD_FAIL,
    ADD_SUCCESS,
    ADD_ALREADYMANAGED,

    REMOVE_FAIL,
    REMOVE_SUCCESS,

    REFRESH_FAIL,
    REFRESH_SUCCESS,
    REFRESH_PARTIAL,

    PULLING_FAIL,
    PULLING_SUCCESS,

    PUSHING_FAIL,
    PUSHING_SUCCESS
};

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
        bool replyClient(OrchestratorClient* &client, const json &result);
        bool handle_CheckOnline(OrchestratorClient* &client);
        bool handle_Restart(OrchestratorClient* &client);
        bool handle_Update(OrchestratorClient* &client);
        bool handle_Discover(OrchestratorClient* &client);
        bool handle_Pull(OrchestratorClient* &client);
        bool handle_Push(OrchestratorClient* &client);
        bool handle_GetLog(OrchestratorClient* &client);

        string mServerStartedTimestamp;

        in_addr mBindAddr{};
        bool resolveInterfaceOrIp(const string& ifaceOrIp, in_addr& out);
        bool setBindInterface(const std::string& ifaceOrIp);

        string mConfigFile = "./Orchestrator.json";
        string mLogFile = "./Orchestrator.log";
        string mBindInterface = "";

        std::string generateRandomID() { std::string result; const std::string valid_characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; std::random_device rd; std::mt19937 generator(rd()); std::uniform_int_distribution<> distribution(0, valid_characters.size() - 1); for (int i = 0; i < 15; ++i) { result += valid_characters[distribution(generator)]; } return result; }
        std::string queryIPAddress(const char* mac_address);
        std::string queryMACAddress(const char* ip_address);

        bool readConfiguration();
        bool saveConfiguration() { ofstream outFile(mConfigFile.c_str()); if (!outFile.is_open()) return false; outFile << Configuration.dump(4); outFile.close(); return !outFile.fail(); }

        inline bool isManaged(const String &target) { return Configuration["Managed Devices"].contains(target); }
        const json getDevice(const String &target);

        void applyBindForUdpSocket(int sockfd);

        void init();
    public:
        explicit OrchestratorServer(const std::string &configfile) : mConfigFile(configfile) { init(); }
        explicit OrchestratorServer(const std::string &configfile, const std::string &bindinfertace) : mConfigFile(configfile), mBindInterface(bindinfertace) { init(); }

        inline std::string ConfigFile() const { return mConfigFile; }
        inline void ConfigFile(const std::string& value) { mConfigFile = value; }

        inline const string &ServerStartedTimestamp() const { return mServerStartedTimestamp; }
        void UpdateStatus(bool status);

        int Manage();
        
        json Query(const std::string& orchestrator_url, uint16_t orchestrator_port, const json& payload);
        bool SendToDevice(const std::string& destination, const json& payload);
        
        bool SaveDeviceConfiguration(const json &cfg);
        const json ReadDeviceConfiguration(const String &target);

        json Configuration;
};

#endif // ORCHESTRATORSERVER_H