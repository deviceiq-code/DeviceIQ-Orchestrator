#include "../include/String.h"

bool String::Equals(const char* str, bool ignorecase) const {
    if (!str) return false;

    if (ignorecase) {
        const char* a = this->c_str();
        const char* b = str;

        while (*a && *b) {
            if (std::tolower(static_cast<unsigned char>(*a)) !=
                std::tolower(static_cast<unsigned char>(*b))) {
                return false;
            }
            ++a;
            ++b;
        }
        return *a == *b;
    }

    return *this == String(str);
}

bool String::Equals(const String other, bool ignorecase) const {
    if (ignorecase) {
        const char* a = this->c_str();
        const char* b = other.c_str();

        while (*a && *b) {
            if (std::tolower(static_cast<unsigned char>(*a)) !=
                std::tolower(static_cast<unsigned char>(*b))) {
                return false;
            }
            ++a;
            ++b;
        }
        return *a == *b;
    }

    return *this == other;
}

std::string String::LowerCase() {
    std::string source = *this;
    std::transform(source.begin(), source.end(), source.begin(), [](unsigned char c) { return std::tolower(c); });
    return source;
}

std::string String::UpperCase() {
    std::string source = *this;
    std::transform(source.begin(), source.end(), source.begin(), [](unsigned char c) { return std::toupper(c); });
    return source;
}

std::string String::LimitString(size_t sz, bool fill) {
    std::string ret = (length() <= sz ? *this : substr(0, sz - 3) + "...");
    if (fill && ret.length() < sz) {
        ret.append(sz - ret.length(), ' ');
    }
    return ret;
}

String::String(std::string str, size_t sz) {
    *this = str.erase(length() - sz);
}

String::String(const char* str, size_t sz) {
    char tmpRet[sz];
    for (uint32_t i = 0; i < sz; i++) tmpRet[i] = (char)str[i];
    tmpRet[sz] = 0;
    *this = std::string(tmpRet);
}

std::vector<std::string> String::Tokenize(const char separator) {
    std::vector<std::string> tokens;
    std::string token;
    std::stringstream ss(*this);

    while (std::getline(ss, token, separator)) {
        tokens.push_back(token);
    }

    return tokens;
}

std::string String::TrimLeft() {
    std::string result = *this;
    result.erase(result.begin(), std::find_if(result.begin(), result.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));

    return result;
}

std::string String::TrimRight() {
    std::string result = *this;
    result.erase(std::find_if(result.rbegin(), result.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), result.end());

    return result;
}

std::string String::Trim() {
    std::string result = *this;

    result.erase(result.begin(), std::find_if(result.begin(), result.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));

    result.erase(std::find_if(result.rbegin(), result.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), result.end());

    return result;
}
