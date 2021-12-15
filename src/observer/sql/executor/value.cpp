/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All
rights reserved. miniob is licensed under Mulan PSL v2. You can use this software according to the
terms and conditions of the Mulan PSL v2. You may obtain a copy of Mulan PSL v2 at:
          http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "exception.h"
#include "value.h"

#include <cmath>


static std::string float2str(double a) {
    int i = static_cast<int>(floor(a));
    int f = static_cast<int>((a - i) * 100);

    if(f == 0) {
        return std::to_string(i);
    } else if(f % 10 == 0) {
        return std::to_string(i) + "." + std::to_string(f / 10);
    } else {
        return std::to_string(i) + "." + std::to_string(f);
    }
}


void IntValue::to_string(std::ostream &os) const { 
    switch (aggregation_type_) {
        case Count:
            os << count;
            break;
        case Avg:
            os << float2str(avg_);
            break;
        default:
            os << value_; 
            break;
    }
}

void IntValue::merge(const TupleValue &other) {
    const IntValue &int_other = (const IntValue &)other;
    switch (aggregation_type_) {
        case Count: {
            count++;
        } break;
        case Sum: {
            value_ = value_ + int_other.value_;
        } break;
        case Avg: {
            int pre_count = count++;
            avg_ = (avg_ * pre_count + int_other.value_) / count;
            std::cout << avg_ << std::endl;
        } break;
        case Max: {
            if (compare(int_other) < 0) value_ = int_other.value_;
        } break;
        case Min: {
            if (compare(int_other) > 0) value_ = int_other.value_;
        }
        default: {
        }
    }
}

void FloatValue::to_string(std::ostream &os) const { 
    switch (aggregation_type_) {
        case Count:
            os << count;
            break;
        case Avg:
            os << float2str(avg_);
            break;
        default:
            os << float2str(value_);
            break;
    }
}

void FloatValue::merge(const TupleValue &other) {
    const FloatValue &int_other = (const FloatValue &)other;
    switch (aggregation_type_) {
        case Count: {
            count++;
        } break;
        case Avg: {
            int pre_count = count++;
            avg_ = (avg_ * pre_count + int_other.value_) / count;
        } break;
        case Max: {
            if (compare(int_other) < 0) value_ = int_other.value_;
        } break;
        case Min: {
            if (compare(int_other) > 0) value_ = int_other.value_;
        }
        default: {
        }
    }
}

void StringValue::to_string(std::ostream &os) const {
    switch (aggregation_type_) {
        case Count:
            os << count;
            break;
        default:
            os << value_;
            break;
    }
}

void StringValue::merge(const TupleValue &other) {
    const StringValue &int_other = (const StringValue &)other;
    switch (aggregation_type_) {
        case Count: {
            count++;
        } break;
        case Avg: {
            throw_error(RC::INVALID_ARGUMENT, "Invalid operation `Avg` on string.");
        } break;
        case Max: {
            if (compare(int_other) < 0) value_ = int_other.value_;
        } break;
        case Min: {
            if (compare(int_other) > 0) value_ = int_other.value_;
        }
        default: {
        }
    }
}
