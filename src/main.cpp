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

string TargetDevice = DEF_BROADCASTADDRESS;
string TargetMAC = "D8:13:2A:7E:EE:C4";
bool Force = DEF_FORCE;
bool Apply = DEF_APPLY;

OrchestratorAction Action = ACTION_NOACTION;
DiscoveryMode Mode = DISCOVERY_NONE;

int main(int argc, char** argv) {
    CommandLineParser clp;

    ServerOrchestrator = new Orchestrator();
    ServerLog = new Log(DEF_LOGFILE, ENDPOINT_CONSOLE);

    // Actions 

    clp.OnAction("refresh", []() { Action = ACTION_REFRESH;});
    clp.OnAction("discover", []() { Action = ACTION_DISCOVERY;});
    clp.OnAction("list", []() { Action = ACTION_LIST;});
    clp.OnAction("add", []() { Action = ACTION_ADD; });
    clp.OnAction("remove", []() { Action = ACTION_REMOVE; });
    clp.OnAction("update", []() { Action = ACTION_UPDATE; });
    clp.OnAction("restart", []() { Action = ACTION_RESTART; });
    clp.OnAction("pull", []() { Action = ACTION_PULL; });
    clp.OnAction("push", []() { Action = ACTION_PUSH; });
    clp.OnAction("manage", []() { Action = ACTION_MANAGE; });
    clp.OnAction("checkonline", []() { Action = ACTION_CHECKONLINE; });
    clp.OnAction("reloadconfig", []() { Action = ACTION_RELOADCONFIG; });

    // Parameters

    clp.OnParameter('v', "version", no_argument, [](char* p_arg) {
        fprintf(stdout, "%s %s version %s\r\n\r\n", Version.ProductFamily.c_str(), Version.ProductName.c_str(), Version.Software.Info().c_str());
        exit(0);
    });

    clp.OnParameter('t', "target", required_argument, [](char* p_arg) {
        if (p_arg[0] == '=') p_arg = ++p_arg;
        TargetDevice = p_arg;

        if (String(TargetDevice).Equals("all")) { Mode = DISCOVERY_ALL; };
        if (String(TargetDevice).Equals("managed")) { Mode = DISCOVERY_MANAGED; };
        if (String(TargetDevice).Equals("unmanaged")) { Mode = DISCOVERY_UNMANAGED; };
    });

    clp.OnParameter('c', "config", required_argument, [&](char* p_arg) {
        if (p_arg[0] == '=') p_arg = ++p_arg;
        ServerOrchestrator->ConfigFile(p_arg);
    });

    clp.OnParameter('f', "force", no_argument, [](char* p_arg) {
        Force = true;
    });

    clp.OnParameter('a', "apply", no_argument, [](char* p_arg) {
        Apply = true;
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

    switch (Action) {
        case ACTION_LIST : {
            ServerOrchestrator->List();
        } break;
        case ACTION_DISCOVERY : {
            ServerOrchestrator->Discovery(TargetDevice);
        } break;
        case ACTION_ADD : {
            if (TargetDevice.find("255") != string::npos) {
                fprintf(stderr, "Invalid target request %s.\r\n\r\n", TargetDevice.c_str());
                exit(1);
            }

            OperationResult r = ServerOrchestrator->Add(TargetDevice.c_str(), 15, Force);
            switch (r) {
                case ADD_SUCCESS : {
                    fprintf(stdout, "Success adding device %s.\r\n\r\n", TargetDevice.c_str());
                } break;
                case ADD_FAIL : {
                    fprintf(stderr, "Error adding device %s.\r\n\r\n", TargetDevice.c_str());
                } break;
                case ADD_ALREADYMANAGED : {
                    fprintf(stderr, "Device %s is already managed by this Orchestrator.\r\n\r\n", TargetDevice.c_str());
                } break;
            }
        } break;
        case ACTION_REMOVE : {
            if (TargetDevice.find("255") != string::npos) {
                fprintf(stderr, "Invalid target request %s.\r\n\r\n", TargetDevice.c_str());
                exit(1);
            }

            OperationResult r = ServerOrchestrator->Remove(TargetDevice, 15, Force);
            switch (r) {
                case REMOVE_SUCCESS : {
                    fprintf(stdout, "Success removing device %s.\r\n\r\n", TargetMAC.c_str());
                } break;
                case REMOVE_FAIL : {
                    fprintf(stderr, "Error removing device %s.\r\n\r\n", TargetMAC.c_str());
                } break;
                case NOTMANAGED : {
                    fprintf(stderr, "Device %s is not managed by this Orchestrator.\r\n\r\n", TargetMAC.c_str());
                } break;
            }
        } break;
        case ACTION_UPDATE : {
            bool r = ServerOrchestrator->Update(TargetDevice.c_str());
            fprintf(stdout, "update command sent.\r\n\r\n");
        } break;
        case ACTION_RESTART : {
            bool r = ServerOrchestrator->Restart(TargetDevice);
            fprintf(stdout, "Finish restarting. %u\r\n\r\n", r);
        } break;
        case ACTION_REFRESH : {
            bool r = ServerOrchestrator->Refresh(TargetDevice);
            fprintf(stdout, "Finish refreshing.\r\n\r\n");
        } break;
        case ACTION_PULL : {
            OperationResult r = ServerOrchestrator->Pull(TargetDevice.c_str(), 15);
            fprintf(stdout, "Finish config pulling.\r\n\r\n");
        } break;
        case ACTION_PUSH : {
            OperationResult r = ServerOrchestrator->Push(TargetDevice.c_str(), 15, Apply);
            fprintf(stdout, "Finish config pushing.\r\n\r\n");
        } break;
        case ACTION_MANAGE : {
            int r = ServerOrchestrator->Manage();
            exit(r);
        } break;
        case ACTION_CHECKONLINE : {
            fprintf(stdout, "Orchestrator server %s is: %s\r\n\r\n", TargetDevice.c_str(), (ServerOrchestrator->CheckOnline(TargetDevice, 30030) ? "online" : "offline"));
            exit(0);
        } break;
        case ACTION_RELOADCONFIG : {
            fprintf(stdout, "Reload config at Orchestrator server %s: %s\r\n\r\n", TargetDevice.c_str(), (ServerOrchestrator->ReloadConfig(TargetDevice, 30030) ? "success" : "fail"));
            exit(0);
        } break;
        default: {
            fprintf(stderr, "Invalid request.\r\n\r\n");
            exit(1);
        }
    }
}