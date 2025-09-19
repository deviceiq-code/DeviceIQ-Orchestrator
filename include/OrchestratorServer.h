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
#include <csignal>

#include <nlohmann/json.hpp>

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
constexpr char DEF_CONFIGFILE[] = "./orchestrator.json";
constexpr char DEF_LOGFILE[] = "./orchestrator.log";
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

class OrchestratorClient {
    private:
        int mID;
        sockaddr_in mInfo;
        string mIncomingBuffer;
        string mOutgoingBuffer;
        json mIncomingJSON;
        json mOutgoingJSON;
        
    public:
        OrchestratorClient(uint32_t id, const sockaddr_in &info) : mID(id), mInfo(info) {}

        bool Reply();

        void IncomingBuffer(const string &value);
        const string IncomingBuffer() { return mIncomingBuffer; }
        void OutgoingBuffer(const string &value);
        void OutgoingBuffer(const json &value);
        const string OutgoingBuffer() { return mOutgoingBuffer; }
        const string IPAddress() { return inet_ntoa(mInfo.sin_addr); }

        const json &IncomingJSON() const noexcept { return mIncomingJSON; }
        const json &OutgoingJSON() const noexcept { return mOutgoingJSON; }
};

class OrchestratorServer {
    private:
        bool replyClient(OrchestratorClient* &client, const json &result);
        bool handle_CheckOnline(OrchestratorClient* &client);
        bool handle_ReloadConfig(OrchestratorClient* &client);
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

        string mConfigFile = DEF_CONFIGFILE;

        string generateRandomID();
        string queryIPAddress(const char* mac_address);
        string queryMACAddress(const char* ip_address);

        inline bool isManaged(const String &target) { return Configuration["Managed Devices"].contains(target); }
        const json getDevice(const String &target);

        void applyBindForUdpSocket(int sockfd);

        void init();
    public:
        explicit OrchestratorServer() { init(); }

        inline std::string ConfigFile() const { return mConfigFile; }
        inline void ConfigFile(const std::string& value) { mConfigFile = value; }

        inline const string &ServerStartedTimestamp() const { return mServerStartedTimestamp; }
        void UpdateStatus(bool status);

        int Manage();
        
        json Query(const std::string& orchestrator_url, uint16_t orchestrator_port, const json& payload);
        bool SendToDevice(const std::string& destination, const json& payload);

        bool CheckOnline(const std::string& orchestrator_url, uint16_t orchestrator_port);
        bool ReloadConfig(const std::string& orchestrator_url, uint16_t orchestrator_port);

        
        bool Initialize();
        bool ReadConfiguration();
        bool SaveConfiguration();
        
        bool SaveDeviceConfiguration(const json &cfg);
        const json ReadDeviceConfiguration(const String &target);

        json Configuration;
};

#endif // ORCHESTRATORSERVER_H