#ifndef Orchestrator_h
#define Orchestrator_h

#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <random>
#include <chrono>
#include <fstream>

#include "Json.h"
#include "String.h"

using json = nlohmann::json;

constexpr size_t DEF_BUFFERSIZE = 1024;
constexpr uint16_t DEF_LISTENTIMEOUT = 5;
constexpr uint16_t DEF_PORT = 30030;
constexpr char DEF_DATETIMEFORMAT[] = "%d/%m/%Y %H:%M:%S";
constexpr char DEF_BROADCASTADDRESS[] = "255.255.255.255";
constexpr char DEF_CONFIGFILENAME[] = "./orchestrator.json";
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
T safeGet(const nlohmann::json& jsonValue, const T& defaultValue = T()) {
    try {
        return jsonValue.get<T>();
    } catch (nlohmann::json::type_error&) {
        return defaultValue;
    } catch (nlohmann::json::out_of_range&) {
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
        std::string mServerName;
        std::string mServerID;
        std::string CurrentConfigurationFile;

        std::string generateRandomID();
        inline long long getCurrentEpochTime() { return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count(); }
        inline std::string formatEpochTime(std::time_t epoch_time, const char* format = DEF_DATETIMEFORMAT) { std::tm* tm_ptr = std::localtime(&epoch_time); std::ostringstream oss; oss << std::put_time(tm_ptr, format); return oss.str(); }
        std::string queryIPAddress(const char* mac_address);
        std::string queryMACAddress(const char* ip_address);
        bool sendMessage(const std::string& message, const uint16_t port, const char* dest_address = DEF_BROADCASTADDRESS);
        void serverListen(const uint16_t port, const uint16_t listen_timeout, std::function<void(Client)> ondata_callback, const size_t bufferSize = DEF_BUFFERSIZE);
        void serverMultipleListen(uint16_t port, uint16_t timeoutSeconds, std::function<void(Client)> callback, const size_t bufferSize);
    public:
        inline std::string ServerID() { return mServerID; }
        inline std::string ServerName() { return mServerName; }
        void List();
        void Discovery(const DiscoveryMode mode, const uint16_t listen_timeout = DEF_LISTENTIMEOUT, const char* dest_address = DEF_BROADCASTADDRESS);
        OperationResult Add(std::string target, const uint16_t listen_timeout = DEF_LISTENTIMEOUT, const bool force = DEF_FORCE);
        OperationResult Remove(std::string target, const uint16_t listen_timeout = DEF_LISTENTIMEOUT, const bool force = DEF_FORCE);
        OperationResult Refresh(std::string target, const uint16_t listen_timeout = DEF_LISTENTIMEOUT);
        OperationResult Pull(std::string target, const uint16_t listen_timeout = DEF_LISTENTIMEOUT);
        OperationResult Push(std::string target, const uint16_t listen_timeout = DEF_LISTENTIMEOUT, const bool apply = DEF_APPLY);
        bool Update(std::string target);
        bool Restart(std::string target);
        bool ReadConfiguration(const char* filename);
        bool SaveConfiguration();

        json Configuration;
};

#endif