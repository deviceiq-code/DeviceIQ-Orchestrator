#include "../include/OrchestratorClient.h"

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
                return false;
            }
            return false;
        }
    }

    if (::shutdown(mID, SHUT_WR) == -1) {
        return false;
    }

    return true;
}