#include "../include/Tools.h"

namespace Tools {
    string CurrentDateTime() {
        using namespace std::chrono;
        auto now = system_clock::now();
        std::time_t t = system_clock::to_time_t(now);
        
        auto us = duration_cast<microseconds>(now.time_since_epoch()) % seconds(1);
        std::tm tm{};
        gmtime_r(&t, &tm);

        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << '.' << std::setw(6) << std::setfill('0') << us.count() << "Z";
        
        return oss.str();
    }
}