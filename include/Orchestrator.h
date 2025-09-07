#ifndef Orchestrator_h
#define Orchestrator_h

#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <net/if.h>
#include <ifaddrs.h>
#include <unistd.h>
#include <thread>
#include <random>
#include <chrono>
#include <fstream>
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
constexpr bool DEF_FORCE = false;
constexpr bool DEF_APPLY = false;

enum OrchestratorAction { ACTION_NOACTION, ACTION_DISCOVERY, ACTION_ADD, ACTION_RESTART, ACTION_REMOVE, ACTION_UPDATE, ACTION_REFRESH, ACTION_LIST, ACTION_PULL, ACTION_PUSH };
enum DiscoveryMode { DISCOVERY_NONE, DISCOVERY_ALL, DISCOVERY_UNMANAGED, DISCOVERY_MANAGED };

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
        if (jsonValue.is_null()) { return defaultValue; }
        return jsonValue.get<T>();
    } catch (const nlohmann::json::type_error&) {
        return defaultValue;
    } catch (const nlohmann::json::out_of_range&) {
        return defaultValue;
    }
}

struct Client {
    uint16_t ID;
    sockaddr_in Info;
    std::string IncomingBuffer;
    std::string OutgoingBuffer;
    void Send(char* reply) { OutgoingBuffer = reply; send(ID, OutgoingBuffer.c_str(), OutgoingBuffer.length(), 0); }
};

struct Device {
    uint16_t ID;
};

class Orchestrator {
    private:
        in_addr mBindAddr{};
        bool resolveInterfaceOrIp(const string& ifaceOrIp, in_addr& out);
        bool setBindInterface(const std::string& ifaceOrIp);

        string mConfigFile = DEF_CONFIGFILE;

        string generateRandomID();
        string queryIPAddress(const char* mac_address);
        string queryMACAddress(const char* ip_address);
        void applyBindForUdpSocket(int sockfd);
        bool sendMessage(const std::string& message, const uint16_t port, const char* dest_address = DEF_BROADCASTADDRESS);
        void serverListen(const uint16_t port, const uint16_t listen_timeout, std::function<void(Client)> ondata_callback, const size_t bufferSize = DEF_BUFFERSIZE);
        void serverMultipleListen(uint16_t port, uint16_t timeoutSeconds, std::function<void(Client)> callback, const size_t bufferSize);

        void init();
    public:
        explicit Orchestrator() { init(); }

        inline std::string ConfigFile() const { return mConfigFile; }
        inline void ConfigFile(const std::string& value) { mConfigFile = value; }

        void List();
        void Discovery(const DiscoveryMode mode, const uint16_t listen_timeout = DEF_LISTENTIMEOUT, const char* dest_address = DEF_BROADCASTADDRESS);
        OperationResult Add(std::string target, const uint16_t listen_timeout = DEF_LISTENTIMEOUT, const bool force = DEF_FORCE);
        OperationResult Remove(std::string target, const uint16_t listen_timeout = DEF_LISTENTIMEOUT, const bool force = DEF_FORCE);
        OperationResult Refresh(std::string target, const uint16_t listen_timeout = DEF_LISTENTIMEOUT);
        OperationResult Pull(std::string target, const uint16_t listen_timeout = DEF_LISTENTIMEOUT);
        OperationResult Push(std::string target, const uint16_t listen_timeout = DEF_LISTENTIMEOUT, const bool apply = DEF_APPLY);
        bool Update(std::string target);
        bool Restart(std::string target);
        
        bool Initialize();
        bool ReadConfiguration();
        bool SaveConfiguration();

        json Configuration;
};

#endif