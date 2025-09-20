#ifndef ORCHESTRATORCLIENT_H
#define ORCHESTRATORCLIENT_H

#include <arpa/inet.h>
#include <poll.h>
#include <nlohmann/json.hpp>

class OrchestratorClient {
    private:
        int mID;
        sockaddr_in mInfo;
        std::string mIncomingBuffer;
        std::string mOutgoingBuffer;
        nlohmann::json mIncomingJSON;
        nlohmann::json mOutgoingJSON;
        
    public:
        OrchestratorClient(uint32_t id, const sockaddr_in &info) : mID(id), mInfo(info) {}

        bool Reply();

        inline void IncomingBuffer(const std::string &value) { mIncomingBuffer = value; mIncomingJSON.clear(); try { mIncomingJSON = nlohmann::json::parse(mIncomingBuffer); } catch (...) { mIncomingJSON.clear(); } }
        const std::string IncomingBuffer() { return mIncomingBuffer; }
        void OutgoingBuffer(const std::string &value) { mOutgoingBuffer = value; mOutgoingJSON.clear(); try { mOutgoingJSON = nlohmann::json::parse(mOutgoingBuffer); } catch (...) {} }
        void OutgoingBuffer(const nlohmann::json &value) { OutgoingBuffer(value.dump()); }
        const std::string OutgoingBuffer() { return mOutgoingBuffer; }
        const std::string IPAddress() { return inet_ntoa(mInfo.sin_addr); }
        const std::string MACAddress() { return inet_ntoa(mInfo.sin_addr); }

        const nlohmann::json &IncomingJSON() const noexcept { return mIncomingJSON; }
        const nlohmann::json &OutgoingJSON() const noexcept { return mOutgoingJSON; }
};

#endif // ORCHESTRATORCLIENT_H