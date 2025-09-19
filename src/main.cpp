#include <stdio.h>
#include <iostream>
#include <unistd.h>
#include <getopt.h>
#include <cstring>
#include <algorithm>
#include <arpa/inet.h>
#include <nlohmann/json.hpp>

#include "../include/String.h"
#include "../include/Version.h"
#include "../include/Log.h"
#include "../include/OrchestratorServer.h"
#include "../include/CommandLineParser.h"

using namespace std;
using json = nlohmann::json;

using namespace Orchestrator_Log;

String ConfigFile = "./Orchestrator.json";
String LogFile = "./Orchestrator.log";
string TargetInterface;

OrchestratorServer *Orchestrator;
Orchestrator_Log::Log *ServerLog;

int main(int argc, char** argv) {
    CommandLineParser clp;

    clp.OnParameter('v', "version", no_argument, [](char* p_arg) {
        fprintf(stdout, "%s %s version %s\r\n\r\n", Version.ProductFamily.c_str(), Version.ProductName.c_str(), Version.Software.Info().c_str());
        exit(0);
    });

    clp.OnParameter('c', "config", required_argument, [&](char* p_arg) {
        if (p_arg[0] == '=') p_arg = ++p_arg;
        ConfigFile = p_arg;
    });

    clp.OnParameter('i', "interface", required_argument, [&](char* p_arg) {
        if (p_arg[0] == '=') p_arg = ++p_arg;
        TargetInterface = p_arg;
    });

    clp.Parse(argc, argv);

    try {
        Orchestrator = new OrchestratorServer(ConfigFile, TargetInterface);
    }
    catch(const std::exception& e) {
        fprintf(stderr, "Error: %s\r\n\r\n", e.what());
        exit(1);
    }

    ServerLog = new Log(JSON<String>(Orchestrator->Configuration["Configuration"]["Log File"], LogFile), JSON<string>(Orchestrator->Configuration["Configuration"]["Log Endpoint"], "ENDPOINT_CONSOLE, ENDPOINT_FILE, ENDPOINT_SYSLOG_LOCAL, ENDPOINT_SYSLOG_REMOTE"));
    ServerLog->SyslogServer(JSON<string>(Orchestrator->Configuration["Configuration"]["Syslog URL"], ""), JSON<uint16_t>(Orchestrator->Configuration["Configuration"]["Syslog Port"], 514));

    ServerLog->Write("Configuration file: " + Orchestrator->ConfigFile(), LOGLEVEL_INFO);
    ServerLog->Write("Server name: " + Orchestrator->Configuration["Configuration"]["Server Name"].get<string>(), LOGLEVEL_INFO);
    ServerLog->Write("Server ID: " + Orchestrator->Configuration["Configuration"]["Server ID"].get<string>(), LOGLEVEL_INFO);
    ServerLog->Write("Server Version: " + Version.Software.Info(), LOGLEVEL_INFO);

    int r = Orchestrator->Manage();
    exit(r);
}