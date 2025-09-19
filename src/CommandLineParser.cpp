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
    // Parameters
    std::string shortopts;

    for (const auto& i : mParameters) {
        shortopts += i.val;
        if (i.has_arg == required_argument) shortopts += ":";
        else if (i.has_arg == optional_argument) shortopts += "::";
    }

    char optc = '\0';
    int idx = 0;
    bool getopt_long_sucess = false;

    while ((optc = getopt_long(argc, argv, shortopts.c_str(), mParameters.data(), &idx)) != -1) {
        for (size_t i = 0; i < mParameters.size(); ++i) {
            if (optc == mParameters[i].val) {
                mParametersCallbacks[i](optarg);
                getopt_long_sucess = true;
                break;
            }
        }
    }

    if ((getopt_long_sucess == false) && (argc > 2)){
        fprintf(stdout, "Incorrect parameters.\r\n\r\n");
        exit(1);
    }
}