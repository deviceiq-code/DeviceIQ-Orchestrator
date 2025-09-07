#ifndef Version_h
#define Version_h

#include <iostream>

using namespace std;

static struct version {
    const string ProductFamily = "DeviceIQ";
    const string ProductName = "Orchestrator";
    struct software {
        const uint8_t Major = 1;
        const uint8_t Minor = 0;
        const uint8_t Revision = 0;
        inline string Info() { return to_string(Major) + "." + to_string(Minor) + "." + to_string(Revision); }
    } Software;
} Version;

#endif