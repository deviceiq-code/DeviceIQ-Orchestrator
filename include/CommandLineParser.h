#ifndef CommandLineParser_h
#define CommandLineParser_h

#include <cstdint>
#include <cstdio>
#include <getopt.h>
#include <unistd.h>
#include <iostream>
#include <functional>
#include "String.h"

class CommandLineParser {
    private:
        std::vector<const char*> mActions;
        std::vector<option> mParameters = {{ 0, 0, 0, 0 }};
        std::vector<std::function<void()>> mActionsCallbacks;
        std::vector<std::function<void(char*)>> mParametersCallbacks;
    public:
        inline CommandLineParser() {}
        inline ~CommandLineParser() {}

        void OnAction(const char* action, std::function<void()> callback);
        void OnParameter(const char short_param, const char* long_param, uint8_t param_type, std::function<void(char*)> callback);

        void Parse(int argc, char** argv);
};

#endif