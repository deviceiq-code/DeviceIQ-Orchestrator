#include "../include/CommandLineParser.h"

void CommandLineParser::OnAction(const char* action, std::function<void()> callback) {
    mActions.push_back(action);
    mActionsCallbacks.push_back(callback);
}

void CommandLineParser::OnParameter(const char short_param, const char* long_param, uint8_t param_type, std::function<void(char*)> callback) {
    mParameters.insert(mParameters.end() - 1, { long_param, param_type, NULL, short_param });
    mParametersCallbacks.push_back(callback);
}

void CommandLineParser::Parse(int argc, char** argv) {    
    if (argc == 1) {
        fprintf(stdout, "Missing parameters.\r\n");
        exit(1);
    }

    // Actions
    uint16_t id = 0;
    for (const auto& i : mActions) {
        String tmpAction(i);
        if (tmpAction.Equals(argv[1])) {
            mActionsCallbacks[id]();
        }
        id++;
    }

    // Parameters
    std::string shortopts;

    for (const auto& i : mParameters) {
        shortopts += i.val;
        if (i.has_arg == required_argument) shortopts += ":";
        else if (i.has_arg == optional_argument) shortopts += "::";
    }

    char optc = '\0';
    int idx = 0;
    bool sucess = false;

    while ((optc = getopt_long(argc, argv, shortopts.c_str(), mParameters.data(), &idx)) != -1) {
        for (size_t i = 0; i < mParameters.size(); ++i) {
            if (optc == mParameters[i].val) {
                mParametersCallbacks[i](optarg);
                sucess = true;
                break;
            }
        }
    }

    if (sucess == false) {
        fprintf(stdout, "Incorrect parameters.\r\n\r\n");
        exit(1);
    }
}