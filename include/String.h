#ifndef String_h
#define String_h

#include <cstdint>
#include <iostream>
#include <unistd.h>
#include <algorithm>
#include <string>
#include <vector>
#include <sstream>
#include <cctype>

class String : public std::string {
    public:
        using std::string::string;

        String() = default;
        String(int n) : std::string(std::to_string(n)) {}
        String(std::string str) : std::string(str) {}
        String(std::string str, size_t sz);
        String(const char* str, size_t sz);

        bool Equals(const char* str, bool ignorecase = false) const;
        bool Equals(const String, bool ignorecase = false) const;
        std::string LimitString(size_t sz, bool fill = false);
        std::string LowerCase();
        std::string UpperCase();
        std::string TrimLeft();
        std::string TrimRight();
        std::string Trim();
        std::vector<std::string> Tokenize(const char separator);
};

#endif