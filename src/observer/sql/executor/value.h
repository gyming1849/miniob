/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All
rights reserved. miniob is licensed under Mulan PSL v2. You can use this software according to the
terms and conditions of the Mulan PSL v2. You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2021/5/14.
//

#ifndef __OBSERVER_SQL_EXECUTOR_VALUE_H_
#define __OBSERVER_SQL_EXECUTOR_VALUE_H_

#include "sql/parser/parse_defs.h"

#include <cstring>
#include <iostream>
#include <string>

class TupleValue {
public:
    TupleValue() = default;
    virtual ~TupleValue() = default;
    virtual TupleValue *clone() const = 0;
    virtual void to_string(std::ostream &os) const = 0;
    virtual int compare(const TupleValue &other) const = 0;
    virtual void merge(const TupleValue &other) = 0;
    AggregationFunc aggregation_type() { return aggregation_type_; }
    void set_aggregation_type(AggregationFunc aggregation) { aggregation_type_ = aggregation; }
    int get_count() const { return count; }

protected:
    AggregationFunc aggregation_type_ = None;
    int count = 1;
};

class IntValue : public TupleValue {
public:
    explicit IntValue(int value) : value_(value), avg_(value) {}

    IntValue *clone() const { return new IntValue(*this); }

    void to_string(std::ostream &os) const override;
    void merge(const TupleValue &other);

    int compare(const TupleValue &other) const override {
        const IntValue &int_other = (const IntValue &)other;
        return value_ - int_other.value_;
    }

private:
    int value_;
    double avg_;
};

class FloatValue : public TupleValue {
public:
    explicit FloatValue(float value) : value_(value), avg_(value) {}

    FloatValue *clone() const { return new FloatValue(*this); }

    void to_string(std::ostream &os) const override;
    void merge(const TupleValue &other);

    int compare(const TupleValue &other) const override {
        const FloatValue &float_other = (const FloatValue &)other;
        float result = value_ - float_other.value_;
        if (result > 0) {
            return 1;
        }
        if (result < 0) {
            return -1;
        }
        return 0;
    }

private:
    float value_;
    double avg_;
};

class StringValue : public TupleValue {
public:
    StringValue(const char *value, int len) : value_(value, len) {}
    explicit StringValue(const char *value) : value_(value) {}

    StringValue *clone() const { return new StringValue(*this); }

    void to_string(std::ostream &os) const override;
    void merge(const TupleValue &other);

    int compare(const TupleValue &other) const override {
        const StringValue &string_other = (const StringValue &)other;
        return strcmp(value_.c_str(), string_other.value_.c_str());
    }

private:
    std::string value_;
};

#endif  //__OBSERVER_SQL_EXECUTOR_VALUE_H_
