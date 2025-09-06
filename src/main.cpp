#include <stdio.h>
#include <iostream>
#include <unistd.h>
#include <getopt.h>
#include <cstring>
#include <algorithm>
#include <arpa/inet.h>

#include "../include/String.h"
#include "../include/Version.h"
#include "../include/Orchestrator.h"
#include "../include/Json.h"
#include "../include/CommandLineParser.h"

using namespace std;
using json = nlohmann::json;

std::string TargetDevice = DEF_BROADCASTADDRESS;
std::string TargetMAC = "D8:13:2A:7E:EE:C4";
std::string ConfigFilename = DEF_CONFIGFILENAME;
uint16_t ListenTimeout = DEF_LISTENTIMEOUT;
uint16_t Port = DEF_PORT;
bool Force = DEF_FORCE;
bool Apply = DEF_APPLY;

OrchestratorAction Action = ACTION_NOACTION;
DiscoveryMode Mode = DISCOVERY_NONE;

int main(int argc, char** argv) {
    CommandLineParser clp;

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

    clp.OnParameter('p', "port", required_argument, [](char* p_arg) {
        if (p_arg[0] == '=') p_arg = ++p_arg;
        Port = clamp(atoi(p_arg), 0, 65535);
    });

    clp.OnParameter('w', "wait", required_argument, [](char* p_arg) {
        if (p_arg[0] == '=') p_arg = ++p_arg;
        ListenTimeout = clamp(atoi(p_arg), 5, 65535);
    });

    clp.OnParameter('c', "config", required_argument, [](char* p_arg) {
        if (p_arg[0] == '=') p_arg = ++p_arg;
        ConfigFilename = p_arg;
    });

    clp.OnParameter('f', "force", no_argument, [](char* p_arg) {
        Force = true;
    });

    clp.OnParameter('a', "apply", no_argument, [](char* p_arg) {
        Apply = true;
    });

    clp.Parse(argc, argv);

    Orchestrator orchestrator;

    if (orchestrator.ReadConfiguration(ConfigFilename.c_str()) == true) {
        fprintf(stdout, "Configuration file: %s\r\n", ConfigFilename.c_str());
        fprintf(stdout, "Server Name: %s\r\n", orchestrator.ServerName().c_str());
        fprintf(stdout, "Server ID: %s\r\n", orchestrator.ServerID().c_str());
    } else {
        fprintf(stderr, "Configuration file: %s [Invalid]\r\n", ConfigFilename.c_str());
    }

    switch (Action) {
        case ACTION_LIST : {
            orchestrator.List();
        } break;
        case ACTION_DISCOVERY : {
            orchestrator.Discovery(Mode, ListenTimeout, TargetDevice.c_str());
        } break;
        case ACTION_ADD : {
            if (TargetDevice.find("255") != string::npos) {
                fprintf(stderr, "Invalid target request %s.\r\n\r\n", TargetDevice.c_str());
                exit(1);
            }

            OperationResult r = orchestrator.Add(TargetDevice.c_str(), ListenTimeout, Force);
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

            OperationResult r = orchestrator.Remove(TargetDevice, ListenTimeout, Force);
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
            bool r = orchestrator.Update(TargetDevice.c_str());
            fprintf(stdout, "update command sent.\r\n\r\n");
        } break;
        case ACTION_RESTART : {
            bool r = orchestrator.Restart(TargetDevice.c_str());
            fprintf(stdout, "Finish restarting.\r\n\r\n");
        } break;
        case ACTION_REFRESH : {
            OperationResult r = orchestrator.Refresh(TargetDevice.c_str(), ListenTimeout);
            fprintf(stdout, "Finish refreshing.\r\n\r\n");
        } break;
        case ACTION_PULL : {
            OperationResult r = orchestrator.Pull(TargetDevice.c_str(), ListenTimeout);
            fprintf(stdout, "Finish config pulling.\r\n\r\n");
        } break;
        case ACTION_PUSH : {
            OperationResult r = orchestrator.Push(TargetDevice.c_str(), ListenTimeout, Apply);
            fprintf(stdout, "Finish config pushing.\r\n\r\n");
        } break;
        default: {
            fprintf(stderr, "Invalid request.\r\n\r\n");
            exit(1);
        }
    }
}