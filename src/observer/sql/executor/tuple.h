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

#ifndef __OBSERVER_SQL_EXECUTOR_TUPLE_H_
#define __OBSERVER_SQL_EXECUTOR_TUPLE_H_

#include "sql/executor/value.h"
#include "sql/parser/parse.h"

#include <functional>
#include <memory>
#include <vector>

class Table;

class Tuple {
public:
    Tuple() = default;

    Tuple(const Tuple &other);

    ~Tuple();

    Tuple(Tuple &&other) noexcept;
    Tuple &operator=(Tuple &&other) noexcept;

    void add(TupleValue *value);
    void add(const std::shared_ptr<TupleValue> &other);
    void add(int value);
    void add(float value);
    void add(const char *s, int len);

    const std::vector<std::shared_ptr<TupleValue>> &values() const { return values_; }

    size_t size() const { return values_.size(); }

    const TupleValue &get(int index) const { return *values_[index]; }

    const std::shared_ptr<TupleValue> &get_pointer(size_t index) const { return values_[index]; }

private:
    std::vector<std::shared_ptr<TupleValue>> values_;
};

class TupleField {
public:
    TupleField(AttrType type, const char *table_name, const char *field_name,
               AggregationFunc aggregation_type)
        : type_(type), table_name_(table_name), field_name_(field_name),
          aggregation_type_(aggregation_type) {}

    AttrType type() const { return type_; }
    const char *table_name() const { return table_name_.c_str(); }
    const char *field_name() const { return field_name_.c_str(); }
    const AggregationFunc aggregation_type() const { return aggregation_type_; }

    std::string to_string() const;

private:
    AttrType type_;
    std::string table_name_;
    std::string field_name_;
    AggregationFunc aggregation_type_;
};

class TupleSchema {
public:
    TupleSchema() = default;
    ~TupleSchema() = default;

    void add(AttrType type, const char *table_name, const char *field_name,
             AggregationFunc aggregation_type);
    void add_if_not_exists(AttrType type, const char *table_name, const char *field_name);
    void append(const TupleSchema &other);

    const std::vector<TupleField> &fields() const { return fields_; }

    const TupleField &field(int index) const { return fields_[index]; }

    size_t index_of_field(const char *table_name, const char *field_name,
                          const AggregationFunc aggregation_type) const;
    void clear() { fields_.clear(); }

    void print(std::ostream &os, bool multi) const;

public:
    static void from_table(const Table *table, TupleSchema &schema);

private:
    std::vector<TupleField> fields_;
};

class TupleSet {
public:
    TupleSet() = default;
    TupleSet(TupleSet &&other);
    explicit TupleSet(const TupleSchema &schema) : schema_(schema) {}
    TupleSet &operator=(TupleSet &&other);

    ~TupleSet() = default;

    void set_schema(const TupleSchema &schema);

    const TupleSchema &get_schema() const;

    void add(Tuple &&tuple);

    void clear();
    void merge(Tuple &&tuple);
    void merge(Tuple &&tuple, int group_index);
    bool is_empty() const;
    size_t size() const;
    const Tuple &get(int index) const;
    const std::vector<Tuple> &tuples() const;

    void sort(size_t size, OrderBy *attrs);

    void print(std::ostream &os, bool multi) const;

public:
    const TupleSchema &schema() const { return schema_; }

private:
    std::vector<Tuple> tuples_;
    TupleSchema schema_;

    std::function<bool(const Tuple &, const Tuple &)> get_cmp_func(const OrderBy &attr);
};

class TupleRecordConverter {
public:
    TupleRecordConverter(Table *table, TupleSet &tuple_set);

    void add_record(const char *record);

private:
    Table *table_;
    TupleSet &tuple_set_;
};

#endif  //__OBSERVER_SQL_EXECUTOR_TUPLE_H_
