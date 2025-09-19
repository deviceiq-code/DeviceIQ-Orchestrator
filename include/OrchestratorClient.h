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

        void IncomingBuffer(const std::string &value);
        const std::string IncomingBuffer() { return mIncomingBuffer; }
        void OutgoingBuffer(const std::string &value);
        void OutgoingBuffer(const nlohmann::json &value);
        const std::string OutgoingBuffer() { return mOutgoingBuffer; }
        const std::string IPAddress() { return inet_ntoa(mInfo.sin_addr); }

        const nlohmann::json &IncomingJSON() const noexcept { return mIncomingJSON; }
        const nlohmann::json &OutgoingJSON() const noexcept { return mOutgoingJSON; }
};

#endif // ORCHESTRATORCLIENT_H