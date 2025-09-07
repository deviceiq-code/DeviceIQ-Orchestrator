#include "../include/Log.h"

using namespace std;
using namespace Tools;
using namespace Orchestrator_Log;

void Log::init() {
    if (mOutputFile.empty()) {
        char exePath[PATH_MAX];

        ssize_t count = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
        if (count != -1) {
            exePath[count] = '\0';
            mOutputFile = "./" + string(basename(exePath)) + ".log";
        } else {
            mOutputFile = "./output.log";
        }
    }
}

bool Log::writeToConsole(const string& message) {
    fprintf(stdout, "%s", message.c_str());
    return true;
}

bool Log::writeToSyslog(const string& message) {
    fprintf(stdout, "%s", message.c_str());
    return true;
}

bool Log::writeToFile(const string& message) {
    fprintf(stdout, "%s", message.c_str());
    return true;
}

bool Log::Write(string message, LogLevels logtype) {
    FILE* output = nullptr;
    char levelChar;
    uint8_t levelSyslog;

    if (mLogLevel & logtype) {
        switch (logtype) {
            case LOGLEVEL_ERROR: {
                levelChar = 'E';
                levelSyslog = LOG_ERR;
                output = stderr;
            } break;
            case LOGLEVEL_WARNING: {
                levelChar = 'W';
                levelSyslog = LOG_WARNING;
                output = stdout; 
            } break;
            case LOGLEVEL_INFO: {
                levelChar = 'I';
                levelSyslog = LOG_INFO;
                output = stdout;
            } break;
            case LOGLEVEL_DEBUG: {
                levelChar = 'D';
                levelSyslog = LOG_DEBUG;
                output = stdout;
            } break;
            default: {
                return true;
            } break;
        }
    }

    bool ret = true;

    message.erase(std::remove(message.begin(), message.end(), '\r'), message.end());
    message.erase(std::remove(message.begin(), message.end(), '\n'), message.end());

    string output_message = "[" + string(1, levelChar) + "][" + CurrentDateTime() + "] " + message + "\r\n";

    if (mEndpoint & ENDPOINT_CONSOLE) {
        fprintf(output, "%s", output_message.c_str());
        fflush(output);
    }

    if (mEndpoint & ENDPOINT_FILE) {
        output = fopen(mOutputFile.c_str(), "a");
        if (!output) {
            ret = false;
        } else {
            fprintf(output, "%s", output_message.c_str());
        
            fflush(output);
            fclose(output);
        }
    }

    if (mEndpoint & ENDPOINT_SYSLOG_LOCAL) {
        openlog("Orchestrator", LOG_PID | LOG_CONS, LOG_USER);
        syslog(levelSyslog, "%s", string("[" + string(1, levelChar) + "] " + message).c_str());
        closelog();
    }

    if (mEndpoint & ENDPOINT_SYSLOG_REMOTE) {
        if (mSyslogServerURL.empty()) {
            ret = false;
        } else {
            string hostname = "-";
            char tmp[1024]; if (gethostname(tmp, sizeof(tmp)) == 0) hostname = tmp;
            
            std::ostringstream line;
            line << '<' << 1 * 8 + levelSyslog << '>' << "1" << ' ' << CurrentDateTime() << ' ' << (hostname.empty() ? "-" : hostname) << ' ' << "Orchestrator" << ' ' << getpid() << ' ' << '-' << ' ' << '-' << ' ' << message;

            std::string payload = line.str();

            addrinfo hints{}, *res = nullptr;
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_DGRAM;
            
            std::string portstr = std::to_string(mSyslogServerPort);
            if (getaddrinfo(mSyslogServerURL.c_str(), portstr.c_str(), &hints, &res) != 0) {
                ret = false;
            } else {
                int sock = -1, rv = -1;
                for (addrinfo* p = res; p; p = p->ai_next) {
                    sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
                    if (sock == -1) continue;
                    ssize_t n = sendto(sock, payload.data(), payload.size(), 0, p->ai_addr, p->ai_addrlen);
                    if (n == (ssize_t)payload.size()) { rv = 0; }
                    close(sock);
                    if (rv == 0) break;
                }
                freeaddrinfo(res);
                ret = (rv != 0 ? false : true);
            }
        }
    }
    return ret;
}

void Log::Endpoint(const string& value) {
    struct ParseEndpointsResult {
        Endpoints value;
        std::vector<std::string> unknown;
    };

    ParseEndpointsResult res{ ENDPOINT_NOLOG, {} };
    auto tokens = tokenize_endpoints(value);

    if (tokens.empty()) { mEndpoint = res.value; return; }
    if (tokens.size() == 1) {
        std::string t = upper_nospace(trim(tokens[0]));
        if (t == "ALL") {
            res.value = static_cast<Endpoints>(ENDPOINT_CONSOLE | ENDPOINT_SYSLOG_LOCAL | ENDPOINT_SYSLOG_REMOTE | ENDPOINT_FILE);
        }
        if (t == "NONE" || t == "NOLOG") {
            res.value = ENDPOINT_NOLOG;
        }
        
        mEndpoint = res.value;
        return;
    }

    for (auto& raw : tokens) {
        std::string key = normalize_token(raw);

        if (key == "ALL") {
            res.value |= static_cast<Endpoints>(ENDPOINT_CONSOLE | ENDPOINT_SYSLOG_LOCAL | ENDPOINT_SYSLOG_REMOTE | ENDPOINT_FILE);
            continue;
        }
        if (key == "NONE" || key == "NOLOG") {
            res.value = ENDPOINT_NOLOG;
            continue;
        }

        auto it = endpoint_map().find(key);
        if (it != endpoint_map().end()) {
            res.value |= it->second;
        } else {
            res.unknown.push_back(raw);
        }
    }
    mEndpoint = res.value;
}