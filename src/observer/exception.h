#pragma once

#include "rc.h"

#include <cstring>
#include <string>
#include <utility>

template<typename... Args> [[noreturn]]
void throw_error(RC ErrCode, const char *fmt, Args... args) {
    static char buffer[1024];
    memset(buffer, 0, sizeof(buffer));
    sprintf(buffer, fmt, args...);
    throw std::make_pair(ErrCode, buffer);
}
