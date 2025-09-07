#ifndef Log_h
#define Log_h

#include <string>
#include <algorithm>
#include <iostream>
#include <syslog.h>
#include <unistd.h>
#include <libgen.h>
#include <bitset>
#include <fstream>
#include <vector>
#include <iostream>
#include <unordered_map>
#include <limits.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include "./Tools.h"

namespace Orchestrator_Log {
    enum Endpoints { ENDPOINT_NOLOG = 0b00000000, ENDPOINT_CONSOLE = 0b00000001, ENDPOINT_SYSLOG_LOCAL = 0b00000010, ENDPOINT_SYSLOG_REMOTE = 0b00000100, ENDPOINT_FILE = 0b00001000 };
    enum LogLevels { LOGLEVEL_ERROR = 0b00000001, LOGLEVEL_WARNING = 0b00000010, LOGLEVEL_INFO = 0b00000100, LOGLEVEL_DEBUG = 0b00001000, LOGLEVEL_ALL = 0b11111111};

    inline Endpoints operator|(Endpoints a, Endpoints b) { return static_cast<Endpoints>(static_cast<int>(a) | static_cast<int>(b)); }
    inline Endpoints& operator|=(Endpoints& a, Endpoints b) { a = a | b; return a; }
    inline Endpoints operator&(Endpoints a, Endpoints b) { return static_cast<Endpoints>(static_cast<int>(a) & static_cast<int>(b)); }
    inline Endpoints& operator&=(Endpoints& a, Endpoints b) { a = a & b; return a; }
    inline LogLevels operator|(LogLevels a, LogLevels b) { return static_cast<LogLevels>(static_cast<int>(a) | static_cast<int>(b)); }
    inline LogLevels& operator|=(LogLevels& a, LogLevels b) { a = a | b; return a; }
    inline LogLevels operator&(LogLevels a, LogLevels b) { return static_cast<LogLevels>(static_cast<int>(a) & static_cast<int>(b)); }
    inline LogLevels& operator&=(LogLevels& a, LogLevels b) { a = a & b; return a; }
    
    class Log {
        private:
            string mSyslogServerURL = "";
            string mOutputFile = "";
            uint16_t mSyslogServerPort = 514;
            Endpoints mEndpoint = ENDPOINT_NOLOG;
            LogLevels mLogLevel = LOGLEVEL_ALL;
            
            void init();

            bool writeToConsole(const string& message);
            bool writeToSyslog(const string& message);
            bool writeToFile(const string& message);
            
            static inline std::vector<std::string> tokenize_endpoints(const std::string& in) { std::vector<std::string> out; std::string token; for (char c : in) { if (c == ',' || c == '|' || std::isspace(static_cast<unsigned char>(c))) { if (!token.empty()) { out.push_back(token); token.clear(); }} else { token.push_back(c); }} if (!token.empty()) out.push_back(token); return out; }
            static inline std::string upper_nospace(std::string s) { s.erase(std::remove_if(s.begin(), s.end(), [](unsigned char c){ return std::isspace(c); }), s.end()); std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::toupper(c); }); return s; }
            static inline std::string trim(const std::string& s) { const auto is_space = [](unsigned char c){ return std::isspace(c); }; auto start = std::find_if_not(s.begin(), s.end(), is_space); auto end = std::find_if_not(s.rbegin(), s.rend(), is_space).base(); if (start >= end) return {}; return std::string(start, end); }
            static inline std::string normalize_token(std::string tok) { tok = upper_nospace(trim(tok)); if (tok.rfind("ENDPOINT_", 0) == std::string::npos) { if (tok == "ALL" || tok == "NONE" || tok == "NOLOG") return tok; tok = "ENDPOINT_" + tok; } return tok; }
            
            static inline const std::unordered_map<std::string, Endpoints>& endpoint_map() {
                static const std::unordered_map<std::string, Endpoints> m = {
                    {"ENDPOINT_NOLOG",         ENDPOINT_NOLOG},
                    {"ENDPOINT_CONSOLE",       ENDPOINT_CONSOLE},
                    {"ENDPOINT_SYSLOG_LOCAL",  ENDPOINT_SYSLOG_LOCAL},
                    {"ENDPOINT_SYSLOG_REMOTE", ENDPOINT_SYSLOG_REMOTE},
                    {"ENDPOINT_FILE",          ENDPOINT_FILE},
                };
                return m;
            }
        public:
            explicit Log() { init(); }
            explicit Log(const string& outputfile, Endpoints endpoint) : mOutputFile(outputfile), mEndpoint(endpoint) { init(); }
            explicit Log(const string& outputfile, const string& endpoint) : mOutputFile(outputfile) { Endpoint(endpoint); init(); }

            void SyslogServerURL(const string& value) { mSyslogServerURL = value; }
            string SyslogServerURL() const { return mSyslogServerURL; }
            void OutputFile(const string& value) { mOutputFile = value; }
            string OutputFile() const { return mOutputFile; }
            void SyslogServerPort(uint16_t value) { mSyslogServerPort = value; }
            uint16_t SyslogServerPort() const { return mSyslogServerPort; }
            void SyslogServer(const string& url, uint16_t port = 514) { mSyslogServerURL = url; mSyslogServerPort = port; }
            Endpoints Endpoint() const { return mEndpoint; }
            void Endpoint(Endpoints value) { mEndpoint = value; }
            void Endpoint(const string& value);
            LogLevels Level() const { return mLogLevel; }
            void Level(LogLevels value) { mLogLevel = value; }

            bool Write(string message, LogLevels logtype);
    };
}

#endif
