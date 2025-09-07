#ifndef Tools_h
#define Tools_h

#include <chrono>
#include <string>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <nlohmann/json.hpp>

#include "Version.h"

using namespace std;

namespace Tools {
    string CurrentDateTime();
}

#endif