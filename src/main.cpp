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
#include "../include/Orchestrator.h"
#include "../include/CommandLineParser.h"

using namespace std;
using json = nlohmann::json;

using namespace Orchestrator_Log;

Orchestrator *ServerOrchestrator;
Orchestrator_Log::Log *ServerLog;

string TargetInterface;

OrchestratorAction Action = ACTION_NOACTION;

int main(int argc, char** argv) {
    CommandLineParser clp;

    ServerOrchestrator = new Orchestrator();
    ServerLog = new Log(DEF_LOGFILE, ENDPOINT_CONSOLE);

    // Parameters

    clp.OnParameter('v', "version", no_argument, [](char* p_arg) {
        fprintf(stdout, "%s %s version %s\r\n\r\n", Version.ProductFamily.c_str(), Version.ProductName.c_str(), Version.Software.Info().c_str());
        exit(0);
    });

    clp.OnParameter('c', "config", required_argument, [&](char* p_arg) {
        if (p_arg[0] == '=') p_arg = ++p_arg;
        ServerOrchestrator->ConfigFile(p_arg);
    });

    clp.OnParameter('i', "interface", required_argument, [&](char* p_arg) {
        if (p_arg[0] == '=') p_arg = ++p_arg;
        TargetInterface = p_arg;
    });

    clp.Parse(argc, argv);

    if (ServerOrchestrator->ReadConfiguration()) {
        if (!JSON<bool>(ServerOrchestrator->Configuration["Configuration"]["Log File Append"])) std::remove("./orchestrator.log");
        ServerLog->Endpoint(JSON<string>(ServerOrchestrator->Configuration["Configuration"]["Log Endpoint"]));
        ServerLog->SyslogServer(JSON<string>(ServerOrchestrator->Configuration["Configuration"]["Syslog URL"]), JSON<uint16_t>(ServerOrchestrator->Configuration["Configuration"]["Syslog Port"]));

        ServerLog->Write("Configuration file: " + ServerOrchestrator->ConfigFile(), LOGLEVEL_INFO);
        ServerLog->Write("Server name: " + ServerOrchestrator->Configuration["Configuration"]["Server Name"].get<string>(), LOGLEVEL_INFO);
        ServerLog->Write("Server ID: " + ServerOrchestrator->Configuration["Configuration"]["Server ID"].get<string>(), LOGLEVEL_INFO);

        if (!TargetInterface.empty()) {
            ServerOrchestrator->Configuration["Configuration"]["Bind"] = TargetInterface;
        }

        if (ServerOrchestrator->Initialize()) {
            ServerLog->Write("Service bind to " + ServerOrchestrator->Configuration["Configuration"]["Bind"].get<string>(), LOGLEVEL_INFO);
        } else {
            ServerLog->Write("Service unable to bind to " + ServerOrchestrator->Configuration["Configuration"]["Bind"].get<string>(), LOGLEVEL_ERROR);
            exit(1);
        }
    } else {
        ServerLog->Write("Configuration file " + ServerOrchestrator->ConfigFile() + " is invalid", LOGLEVEL_ERROR);
        exit(1);
    }

    int r = ServerOrchestrator->Manage();
    exit(r);
}