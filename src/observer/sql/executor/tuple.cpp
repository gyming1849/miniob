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

#include "sql/executor/tuple.h"
#include "common/log/log.h"
#include "storage/common/table.h"

#include <algorithm>

Tuple::Tuple(const Tuple &other) {
    LOG_PANIC("Copy constructor of tuple is not supported");
    exit(1);
}

Tuple::Tuple(Tuple &&other) noexcept : values_(std::move(other.values_)) {}

Tuple &Tuple::operator=(Tuple &&other) noexcept {
    if (&other == this) {
        return *this;
    }

    values_.clear();
    values_.swap(other.values_);
    return *this;
}

Tuple::~Tuple() {}

// add (Value && value)
void Tuple::add(TupleValue *value) { values_.emplace_back(value); }
void Tuple::add(const std::shared_ptr<TupleValue> &other) { values_.emplace_back(other); }
void Tuple::add(int value) { add(new IntValue(value)); }

void Tuple::add(float value) { add(new FloatValue(value)); }

void Tuple::add(const char *s, int len) { add(new StringValue(s, len)); }

////////////////////////////////////////////////////////////////////////////////

std::string TupleField::to_string() const {
    return std::string(table_name_) + "." + field_name_ + std::to_string(type_);
}

////////////////////////////////////////////////////////////////////////////////
void TupleSchema::from_table(const Table *table, TupleSchema &schema) {
    const char *table_name = table->name();
    const TableMeta &table_meta = table->table_meta();
    const int field_num = table_meta.field_num();
    for (int i = 0; i < field_num; i++) {
        const FieldMeta *field_meta = table_meta.field(i);
        if (field_meta->visible()) {
            schema.add(field_meta->type(), table_name, field_meta->name(), AggregationFunc::None);
        }
    }
}

void TupleSchema::add(AttrType type, const char *table_name, const char *field_name,
                      AggregationFunc aggregation_type) {
    fields_.emplace_back(type, table_name, field_name, aggregation_type);
}

void TupleSchema::add_if_not_exists(AttrType type, const char *table_name, const char *field_name) {
    for (const auto &field : fields_) {
        if (0 == strcmp(field.table_name(), table_name) &&
            0 == strcmp(field.field_name(), field_name)) {
            return;
        }
    }

    add(type, table_name, field_name, AggregationFunc::None);
}

void TupleSchema::append(const TupleSchema &other) {
    fields_.reserve(fields_.size() + other.fields_.size());
    for (const auto &field : other.fields_) {
        fields_.emplace_back(field);
    }
}

size_t TupleSchema::index_of_field(const char *table_name, const char *field_name,
                                   const AggregationFunc agg_type) const {
    for (size_t i = 0; i < fields_.size(); i++) {
        const TupleField &field = fields_[i];
        if (0 == strcmp(field.table_name(), table_name) &&
            0 == strcmp(field.field_name(), field_name) && field.aggregation_type() == agg_type) {
            return i;
        }
    }
    return -1;
}
std::string get_aggregation(AggregationFunc a, std::string fieldname) {
    std::string aggregation = "";
    if (a == AggregationFunc::Avg) aggregation = "Avg";
    if (a == AggregationFunc::Count) aggregation = "Count";
    if (a == AggregationFunc::Max) aggregation = "Max";
    if (a == AggregationFunc::Min) aggregation = "Min";
    if (a == AggregationFunc::Sum) aggregation = "Sum";
    if (a == AggregationFunc::None) return fieldname;
    return aggregation + std::string("(") + fieldname + std::string(")");
}
void TupleSchema::print(std::ostream &os, bool multi) const {
    if (fields_.empty()) {
        os << "No schema";
        return;
    }

    // 判断有多张表还是只有一张表
    std::set<std::string> table_names;
    for (const auto &field : fields_) {
        table_names.insert(field.table_name());
    }

    for (std::vector<TupleField>::const_iterator iter = fields_.begin(), end = --fields_.end();
         iter != end; ++iter) {

        std::string fieldname;
        if (table_names.size() > 1) {
            fieldname = std::string(iter->table_name()) + ".";
        }
        fieldname += std::string(iter->field_name());

        os<<get_aggregation(iter->aggregation_type(),fieldname);

        os << " | ";
    }
    std::string fieldname;
    
    if (multi) {
        // os << fields_.back().table_name() << ".";
        fieldname = std::string(fields_.back().table_name()) + ".";
    }
    fieldname+=fields_.back().field_name();
    os << get_aggregation(fields_.back().aggregation_type(),fieldname) << std::endl;
}

/////////////////////////////////////////////////////////////////////////////
TupleSet::TupleSet(TupleSet &&other) : tuples_(std::move(other.tuples_)), schema_(other.schema_) {
    other.schema_.clear();
}

TupleSet &TupleSet::operator=(TupleSet &&other) {
    if (this == &other) {
        return *this;
    }

    schema_.clear();
    schema_.append(other.schema_);
    other.schema_.clear();

    tuples_.clear();
    tuples_.swap(other.tuples_);
    return *this;
}

void TupleSet::add(Tuple &&tuple) { tuples_.emplace_back(std::move(tuple)); }

void TupleSet::merge(Tuple &&tuple) {
    if (tuples_.empty()) {
        tuples_.emplace_back(std::move(tuple));
        return;
    }
    // 聚合运算的每一个阶段，tuples_ 都只含有一个 Tuple
    const std::vector<std::shared_ptr<TupleValue>> old_values = tuples_[0].values();
    int value_idx = 0;
    for (std::shared_ptr<TupleValue> old_value : old_values) {
        if (old_value->aggregation_type() == None) {
            tuples_.emplace_back(std::move(tuple));
            return;
        }
        old_value->merge(tuple.get(value_idx++));
    }
    return;
}

void TupleSet::merge(Tuple &&tuple, int group_index) {
    if (tuples_.empty() || group_index == -1) {
        tuples_.emplace_back(std::move(tuple));
        return;
    }
    const std::vector<std::shared_ptr<TupleValue>> old_values = tuples_[group_index].values();
    int value_idx = 0;
    for (std::shared_ptr<TupleValue> old_value : old_values) {
        old_value->merge(tuple.get(value_idx++));
        old_value->to_string(std::cout);
    }
    return;
}

void TupleSet::clear() {
    tuples_.clear();
    schema_.clear();
}

/**
 * @brief Generate a funtion that to be used as the third parameter of std::stable_sort.
 *
 * @param attr Sort key.
 * @return std::function<bool(const Tuple &, const Tuple &)>
 */
std::function<bool(const Tuple &, const Tuple &)> TupleSet::get_cmp_func(const OrderBy &attr) {
    int index = schema_.index_of_field(attr.attr.relation_name, attr.attr.attribute_name,
                                       AggregationFunc::None);
    assert(index != -1);
    return [attr, index](const Tuple &u, const Tuple &v) -> bool {
        int res = u.values().at(index)->compare(*v.values().at(index));
        if (attr.order_type == OrderType::Asc) {
            return res < 0;
        } else {
            return res > 0;
        }
    };
}

/**
 * @brief Sort a tupleset according to given rules.
 *
 * @param size Number of rules.
 * @param attrs Array of rules.
 */
void TupleSet::sort(size_t size, OrderBy *attrs) {
    for (size_t i = 0; i < size; i++) {
        std::stable_sort(tuples_.begin(), tuples_.end(), get_cmp_func(attrs[i]));
    }
}

void TupleSet::print(std::ostream &os, bool multi) const {
    if (schema_.fields().empty()) {
        LOG_WARN("Got empty schema");
        return;
    }

    schema_.print(os, multi);

    for (const Tuple &item : tuples_) {
        const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
        if (values.empty()) continue;

        auto size = schema_.fields().size();
        // LOG_WARN("%d %d",values.size(),size);
        for (size_t i = 0; i < size - 1; i++) {
            const auto &value = values[i];
            value->to_string(os);
            os << " | ";
        }
        // LOG_WARN("%d %d",values.size(),size);
        values[size - 1]->to_string(os);
        // LOG_WARN("%d %d",values.size(),size);
        os << std::endl;
    }
    // LOG_WARN("print end");
}

void TupleSet::set_schema(const TupleSchema &schema) { schema_ = schema; }

const TupleSchema &TupleSet::get_schema() const { return schema_; }

bool TupleSet::is_empty() const { return tuples_.empty(); }

size_t TupleSet::size() const { return tuples_.size(); }

const Tuple &TupleSet::get(int index) const { return tuples_[index]; }

const std::vector<Tuple> &TupleSet::tuples() const { return tuples_; }

/////////////////////////////////////////////////////////////////////////////
TupleRecordConverter::TupleRecordConverter(Table *table, TupleSet &tuple_set)
    : table_(table), tuple_set_(tuple_set) {}

void TupleRecordConverter::add_record(const char *record) {
    const TupleSchema &schema = tuple_set_.schema();
    Tuple tuple;
    const TableMeta &table_meta = table_->table_meta();
    for (const TupleField &field : schema.fields()) {
        const FieldMeta *field_meta = table_meta.field(field.field_name());
        // std::cout<<field.field_name()<<"\n";
        assert(field_meta != nullptr);
        switch (field_meta->type()) {
            case INTS: {
                int value = *(int *)(record + field_meta->offset());
                tuple.add(value);
            } break;
            case FLOATS: {
                float value = *(float *)(record + field_meta->offset());
                tuple.add(value);
            } break;
            case CHARS: {
                const char *s = record + field_meta->offset();  // 现在当做Cstring来处理
                tuple.add(s, strlen(s));
            } break;
            default: {
                LOG_PANIC("Unsupported field type. type=%d", field_meta->type());
            }
        }
    }

    tuple_set_.add(std::move(tuple));
}
