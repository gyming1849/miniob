/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All
rights reserved. miniob is licensed under Mulan PSL v2. You can use this software according to the
terms and conditions of the Mulan PSL v2. You may obtain a copy of Mulan PSL v2 at:
          http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Longda on 2021/4/13.
//

#include <set>
#include <sstream>
#include <string>

#include "exception.h"
#include "execute_stage.h"

#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/seda/timer_stage.h"
#include "event/execution_plan_event.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "event/storage_event.h"
#include "session/session.h"
#include "sql/executor/execution_node.h"
#include "sql/executor/tuple.h"
#include "storage/common/condition_filter.h"
#include "storage/common/table.h"
#include "storage/default/default_handler.h"
#include "storage/trx/trx.h"

using namespace common;

RC create_selection_executor(Trx *trx, Selects &selects, const char *db, const char *table_name,
                             SelectExeNode &select_node);

//! Constructor
ExecuteStage::ExecuteStage(const char *tag) : Stage(tag) {}

//! Destructor
ExecuteStage::~ExecuteStage() {}

//! Parse properties, instantiate a stage object
Stage *ExecuteStage::make_stage(const std::string &tag) {
    ExecuteStage *stage = new (std::nothrow) ExecuteStage(tag.c_str());
    if (stage == nullptr) {
        LOG_ERROR("new ExecuteStage failed");
        return nullptr;
    }
    stage->set_properties();
    return stage;
}

//! Set properties for this object set in stage specific properties
bool ExecuteStage::set_properties() {
    //  std::string stageNameStr(stageName);
    //  std::map<std::string, std::string> section = theGlobalProperties()->get(
    //    stageNameStr);
    //
    //  std::map<std::string, std::string>::iterator it;
    //
    //  std::string key;

    return true;
}

//! Initialize stage params and validate outputs
bool ExecuteStage::initialize() {
    LOG_TRACE("Enter");

    std::list<Stage *>::iterator stgp = next_stage_list_.begin();
    default_storage_stage_ = *(stgp++);
    mem_storage_stage_ = *(stgp++);

    LOG_TRACE("Exit");
    return true;
}

//! Cleanup after disconnection
void ExecuteStage::cleanup() {
    LOG_TRACE("Enter");

    LOG_TRACE("Exit");
}

void ExecuteStage::handle_event(StageEvent *event) {
    LOG_TRACE("Enter\n");

    handle_request(event);

    LOG_TRACE("Exit\n");
    return;
}

void ExecuteStage::callback_event(StageEvent *event, CallbackContext *context) {
    LOG_TRACE("Enter\n");

    // here finish read all data from disk or network, but do nothing here.
    ExecutionPlanEvent *exe_event = static_cast<ExecutionPlanEvent *>(event);
    SQLStageEvent *sql_event = exe_event->sql_event();
    sql_event->done_immediate();

    LOG_TRACE("Exit\n");
    return;
}

void ExecuteStage::handle_request(common::StageEvent *event) {
    ExecutionPlanEvent *exe_event = static_cast<ExecutionPlanEvent *>(event);
    SessionEvent *session_event = exe_event->sql_event()->session_event();
    Query *sql = exe_event->sqls();
    const char *current_db = session_event->get_client()->session->get_current_db().c_str();

    CompletionCallback *cb = new (std::nothrow) CompletionCallback(this, nullptr);
    if (cb == nullptr) {
        LOG_ERROR("Failed to new callback for ExecutionPlanEvent");
        exe_event->done_immediate();
        return;
    }
    exe_event->push_callback(cb);

    switch (sql->flag) {
        case SCF_SELECT: {  // select
            try {
                do_select(current_db, sql, exe_event->sql_event()->session_event());
            } catch (std::pair<RC, char *> &err_info) {
                std::stringstream ss;
                ss << "Error(" << (int)err_info.first << "): " << err_info.second << "\n";
                session_event->set_response(ss.str());
            }
            exe_event->done_immediate();
        } break;

        case SCF_INSERT:
        case SCF_UPDATE:
        case SCF_DELETE:
        case SCF_CREATE_TABLE:
        case SCF_SHOW_TABLES:
        case SCF_DESC_TABLE:
        case SCF_DROP_TABLE:
        case SCF_CREATE_INDEX:
        case SCF_DROP_INDEX:
        case SCF_LOAD_DATA: {
            StorageEvent *storage_event = new (std::nothrow) StorageEvent(exe_event);
            if (storage_event == nullptr) {
                LOG_ERROR("Failed to new StorageEvent");
                event->done_immediate();
                return;
            }

            default_storage_stage_->handle_event(storage_event);
        } break;
        case SCF_SYNC: {
            RC rc = DefaultHandler::get_default().sync();
            session_event->set_response(strrc(rc));
            exe_event->done_immediate();
        } break;
        case SCF_BEGIN: {
            session_event->get_client()->session->set_trx_multi_operation_mode(true);
            session_event->set_response(strrc(RC::SUCCESS));
            exe_event->done_immediate();
        } break;
        case SCF_COMMIT: {
            Trx *trx = session_event->get_client()->session->current_trx();
            RC rc = trx->commit();
            session_event->get_client()->session->set_trx_multi_operation_mode(false);
            session_event->set_response(strrc(rc));
            exe_event->done_immediate();
        } break;
        case SCF_ROLLBACK: {
            Trx *trx = session_event->get_client()->session->current_trx();
            RC rc = trx->rollback();
            session_event->get_client()->session->set_trx_multi_operation_mode(false);
            session_event->set_response(strrc(rc));
            exe_event->done_immediate();
        } break;
        case SCF_HELP: {
            const char *response = "show tables;\n"
                                   "desc `table name`;\n"
                                   "create table `table name` (`column name` `column type`, ...);\n"
                                   "create index `index name` on `table` (`column`);\n"
                                   "insert into `table` values(`value1`,`value2`);\n"
                                   "update `table` set column=value [where `column`=`value`];\n"
                                   "delete from `table` [where `column`=`value`];\n"
                                   "select [ * | `columns` ] from `table`;\n";
            session_event->set_response(response);
            exe_event->done_immediate();
        } break;
        case SCF_EXIT: {
            // do nothing
            const char *response = "Unsupported\n";
            session_event->set_response(response);
            exe_event->done_immediate();
        } break;
        default: {
            exe_event->done_immediate();
            LOG_ERROR("Unsupported command=%d\n", sql->flag);
        }
    }
}

void end_trx_if_need(Session *session, Trx *trx, bool all_right) {
    if (!session->is_trx_multi_operation_mode()) {
        if (all_right) {
            trx->commit();
        } else {
            trx->rollback();
        }
    }
}
static RC schema_add_field(Table *table, const char *field_name, AggregationFunc aggregation_type,
                           TupleSchema &schema) {
    const FieldMeta *field_meta = table->table_meta().field(field_name);
    if (nullptr == field_meta) {
        LOG_WARN("No such field. %s.%s", table->name(), field_name);
        return RC::SCHEMA_FIELD_MISSING;
    }
    if (aggregation_type == None)
        schema.add_if_not_exists(field_meta->type(), table->name(), field_meta->name());
    else
        schema.add(field_meta->type(), table->name(), field_meta->name(), aggregation_type);
    return RC::SUCCESS;
}

RC ExecuteStage::check_groupby(const Selects &selects, Table **tables) {
    for (size_t i = 0; i < selects.groupby_num; i++) {
        const RelAttr &attr = selects.groupby_attrs[i];
        if (attr.relation_name == nullptr) {
            throw_error(RC::SCHEMA_TABLE_NAME_ILLEGAL, "GroupBy missing relation name");
        }
        int flag = 0;
        for (size_t j = 0; j < selects.relation_num; j++) {
            if (strcmp(attr.relation_name, selects.relations[j]) == 0) {
                if (tables[j]->table_meta().field(attr.attribute_name) == nullptr) {
                    throw_error(RC::SCHEMA_FIELD_NOT_EXIST, "No such field [%s] in table [%s]",
                                attr.attribute_name, attr.relation_name);
                }
                flag = 1;
                break;
            }
        }
        if (flag == 0) {
            throw_error(RC::SCHEMA_TABLE_NAME_ILLEGAL, "No such table [%s] in GroupBy's relations",
                        attr.relation_name);
        }
    }
    return RC::SUCCESS;
}

RC ExecuteStage::check_attr(const Selects &selects, Table **tables, TupleSchema &schema_result) {
    for (int i = selects.attr_num - 1; i >= 0; i--) {
        const RelAttr &attr = selects.attributes[i];
        if (attr.relation_name != nullptr) {
            int flag = 0;
            for (int j = 0; j < (int)selects.relation_num; j++) {
                if (strcmp(attr.relation_name, selects.relations[j]) == 0) {
                    // printf("%s %s\n",attr.relation_name,selects.relations[j]);
                    flag = 1;
                    if (strcmp("*", attr.attribute_name) == 0) {
                        TupleSchema tmp;
                        TupleSchema::from_table(tables[j], tmp);
                        schema_result.append(tmp);
                    } else if (tables[j]->table_meta().field(attr.attribute_name) == nullptr) {
                        LOG_WARN("No such field [%s] in table [%s]", attr.attribute_name,
                                 attr.relation_name);
                        return RC::SCHEMA_FIELD_NOT_EXIST;
                    } else
                        schema_add_field(tables[j], attr.attribute_name, attr.aggregation_type,
                                         schema_result);
                }
                else
                {
                    // printf("%s %s\n",attr.relation_name,selects.relations[j]);
                }
            }
            if (flag == 0) {
                LOG_WARN("No such table [%s] in Select's relations", attr.relation_name);
                return RC::MISMATCH;
            }
        } else if (attr.aggregation_type == None && strcmp("*", attr.attribute_name) == 0) {
            for (int j = selects.relation_num - 1; j >= 0; j--) {
                TupleSchema tmp;
                TupleSchema::from_table(tables[j], tmp);
                schema_result.append(tmp);
            }
        } else if (attr.aggregation_type != None &&
                   (attr.is_const == 1 && strcmp("*", attr.attribute_name) == 0)) {
            auto field = tables[0]->table_meta().field(0);
            schema_result.add(field->type(), tables[0]->name(), attr.attribute_name,
                              attr.aggregation_type);
        }
        if (attr.aggregation_type == AggregationFunc::None) {
            int flag = 0;
            for (size_t j = 0; j < selects.groupby_num; j++) {
                const RelAttr &groupby_attr = selects.groupby_attrs[j];
                if (strcmp(attr.attribute_name, groupby_attr.attribute_name) == 0) {
                    flag = 1;
                    break;
                }
            }
            if (selects.groupby_num > 0 && flag == 0) {
                LOG_WARN("Select attribute [%s] is not in groupby", attr.attribute_name);
                return RC::MISMATCH;
            }
        }
    }
    return RC::SUCCESS;
}

/**
 * @brief Initialize a select query and validate for table/field names.
 *
 * @param db
 * @param selects
 * @param tables
 * @param schema_result Header of the result table.
 * @return RC
 */
RC ExecuteStage::init_select(const char *db, const Selects &selects, Table **tables,
                             TupleSchema &schema_result) {
    // validator for <from-list>
    for (size_t i = 0; i < selects.relation_num; i++) {
        tables[i] = DefaultHandler::get_default().find_table(db, selects.relations[i]);
        if (tables[i] == nullptr) {
            throw_error(RC::SCHEMA_TABLE_NOT_EXIST, "No such table [%s] in db [%s]",
                        selects.relations[i], db);
        }
    }

    RC rc = check_groupby(selects, tables);
    if (rc != RC::SUCCESS) {
        return rc;
    }
    rc = check_attr(selects, tables, schema_result);
    if (rc != RC::SUCCESS) {
        return rc;
    }

    // validator for <condition>
    size_t left_idx = 0, right_idx = 0;
    for (size_t i = 0; i < selects.condition_num; i++) {
        const auto &cur = selects.conditions[i];
        const auto &left = cur.left_attr, &right = cur.right_attr;
        bool left_flag = false, right_flag = false;

        if (left.relation_name == nullptr || right.relation_name == nullptr) {
            throw_error(RC::SCHEMA_TABLE_NAME_ILLEGAL, "Condition #%lu missing relation name",
                        selects.condition_num - i);
        }

        for (int j = 0; j < (int)selects.relation_num; j++) {
            if (cur.left_is_attr == 1) {
                if (strcmp(left.relation_name, selects.relations[j]) == 0) {
                    left_flag = true;
                    left_idx = j;
                }
            }
            if (cur.right_is_attr == 1) {
                if (strcmp(right.relation_name, selects.relations[j]) == 0) {
                    right_flag = true;
                    right_idx = j;
                }
            }
        }
        if (cur.left_is_attr == 1 && left.relation_name != nullptr) {
            if (!left_flag) {
                throw_error(RC::MISMATCH, "left attr's relation [%s] is not exist",
                            left.relation_name);
            }
            if (tables[left_idx]->table_meta().field(left.attribute_name) == nullptr) {
                throw_error(RC::SCHEMA_FIELD_NOT_EXIST, "left attr [%s] is not exist in [%s]",
                            left.attribute_name, left.relation_name);
            }
        }
        if (cur.right_is_attr == 1 && right.relation_name != nullptr) {
            if (!right_flag) {
                throw_error(RC::MISMATCH, "right attr's relation [%s] is not exist",
                            right.relation_name);
            }
            if (tables[right_idx]->table_meta().field(right.attribute_name) == nullptr) {
                throw_error(RC::SCHEMA_FIELD_NOT_EXIST, "right attr [%s] is not exist in [%s]",
                            right.attribute_name, right.relation_name);
            }
        }
    }

    // validator for <orderby-list>
    for (size_t i = 0; i < selects.orderby_num; i++) {
        const OrderBy &order_by = selects.orderby_attrs[i];
        const RelAttr &attr = order_by.attr;
        if (attr.relation_name != nullptr) {
            int flag = 0;
            for (size_t j = 0; j < selects.relation_num; j++) {
                if (strcmp(attr.relation_name, selects.relations[j]) == 0) {
                    if (nullptr == tables[j]->table_meta().field(attr.attribute_name)) {
                        throw_error(RC::SCHEMA_FIELD_NOT_EXIST, "No such field [%s] in table [%s]",
                                    attr.attribute_name, attr.relation_name);
                    }
                    flag = 1;
                    break;
                }
            }
            if (flag == 0) {
                throw_error(RC::SCHEMA_TABLE_NAME_ILLEGAL,
                            "Relation [%s] in order-by list doesn't exist", attr.relation_name);
            }
        } else {
            throw_error(RC::SCHEMA_TABLE_NAME_ILLEGAL, "Orderby #%lu missing relation name",
                        selects.orderby_num - i);
        }
    }
    return RC::SUCCESS;
}

/**
 * @brief Check whether a select query is about a single table.
 *
 * @param selects The query to be determined.
 * @return std::pair<bool, std::string>
 */
std::pair<bool, std::string> is_single_query(const Selects &selects) {
    std::set<std::string> table_names;
    for (size_t i = 0; i < selects.relation_num; i++) {
        const char *table_name = selects.relations[i];
        if (table_name != nullptr) table_names.insert(std::string(table_name));
    }
    if (table_names.size() == 1) return std::make_pair(true, (*table_names.begin()));
    return std::make_pair(false, std::string());
}

void do_groupby(const TupleSet &all_tuples, TupleSet &output_tuples,
                const TupleSchema group_by_schema) {
    const TupleSchema &schema = all_tuples.schema();
    const auto &output_schema = output_tuples.schema();
    const auto &output_fields = output_schema.fields();
    int output_size = output_fields.size();
    for (auto &all_tuple : all_tuples.tuples()) {
        Tuple output_tuple;
        for (int i = 0; i < output_size; i++) {
            const auto &field = output_fields[i];
            int val_idx = schema.index_of_field(field.table_name(), field.field_name(), None);
            TupleValue *temp_val = all_tuple.get(val_idx).clone();
            temp_val->set_aggregation_type(field.aggregation_type());
            output_tuple.add(temp_val);
        }
        int group_index = -1;
        for (size_t i = 0; i < output_tuples.size(); i++) {
            bool equal = true;
            const auto &group_by_fields = group_by_schema.fields();
            for (const auto &field : group_by_fields) {
                int field_index = -1;

                for (int j = output_size - 1; j >= 0; j--) {
                    if (strcmp(field.table_name(), output_schema.fields()[j].table_name()) == 0 &&
                        strcmp(field.field_name(), output_schema.fields()[j].field_name()) == 0) {
                        field_index = j;
                        break;
                    }
                }

                const auto &in_output = output_tuples.get(i).get(field_index);
                const auto &not_in_output = output_tuple.get(field_index);

                if (in_output.compare(not_in_output) != 0) {
                    equal = false;
                    break;
                }
            }
            if (equal) {
                group_index = i;
                break;
            }
        }
        output_tuples.merge(std::move(output_tuple), group_index);
    }
}

/**
 * @brief Execute a query.
 * 这里没有对输入的某些信息做合法性校验，比如查询的列名、where条件中的列名等，没有做必要的合法性校验
 * 需要补充上这一部分. 校验部分也可以放在resolve，不过跟execution放一起也没有关系
 *
 * @param db
 * @param sql The query to be executed.
 * @param session_event
 * @return RC
 */
RC ExecuteStage::do_select(const char *db, Query *sql, SessionEvent *session_event) {
    RC rc = RC::SUCCESS;
    Session *session = session_event->get_client()->session;
    Trx *trx = session->current_trx();
    Selects &selects = sql->sstr.selection;
    std::pair<bool, std::string> single_query = is_single_query(selects);
    if (single_query.first) {
        for (int i = 0; i < (int)selects.groupby_num; i++) {
            auto &attr = selects.groupby_attrs[i];
            if (!attr.is_const && attr.relation_name == nullptr) {
                attr.relation_name = strdup(single_query.second.c_str());
            }
        }
        for (int i = 0; i < (int)selects.orderby_num; i++) {
            auto &attr = selects.orderby_attrs[i].attr;
            if (!attr.is_const && attr.relation_name == nullptr) {
                attr.relation_name = strdup(single_query.second.c_str());
            }
        }
        for (int i = 0; i < (int)selects.attr_num; i++) {
            auto &attr = selects.attributes[i];
            if (!attr.is_const && attr.relation_name == nullptr) {
                attr.relation_name = strdup(single_query.second.c_str());
            }
        }
    }

    Table *tables[selects.relation_num];
    TupleSchema schema_result;
    rc = init_select(db, selects, tables, schema_result);
    if (rc != RC::SUCCESS) {
        return rc;
    }

    // 把所有的表和只跟这张表关联的 condition 都拿出来，生成最底层的 select 执行节点
    std::vector<SelectExeNode *> select_nodes;
    for (size_t i = 0; i < selects.relation_num; i++) {
        const char *table_name = selects.relations[i];
        SelectExeNode *select_node = new SelectExeNode;
        rc = create_selection_executor(trx, selects, db, table_name, *select_node);
        if (rc != RC::SUCCESS) {
            delete select_node;
            for (SelectExeNode *&tmp_node : select_nodes) {
                delete tmp_node;
            }
            end_trx_if_need(session, trx, false);
            return rc;
        }
        select_nodes.push_back(select_node);
    }

    if (select_nodes.empty()) {
        LOG_ERROR("No table given");
        end_trx_if_need(session, trx, false);
        return RC::SQL_SYNTAX;
    }

    std::vector<TupleSet> tuple_sets;
    for (SelectExeNode *&node : select_nodes) {
        TupleSet tuple_set;
        rc = node->execute(tuple_set);
        if (rc != RC::SUCCESS) {
            for (SelectExeNode *&tmp_node : select_nodes) {
                delete tmp_node;
            }
            end_trx_if_need(session, trx, false);
            return rc;
        } else {
            tuple_sets.push_back(std::move(tuple_set));
        }
    }
    TupleSchema groupby_schema;
    for (int i = selects.groupby_num - 1; i >= 0; i--) {
        const RelAttr &attr = selects.groupby_attrs[i];
        if (selects.relation_num > 1) {
            groupby_schema.add(UNDEFINED, attr.relation_name, attr.attribute_name, None);
        } else {
            groupby_schema.add(UNDEFINED, selects.relations[0], attr.attribute_name, None);
        }
    }
    // puts("Enter");
    std::stringstream ss;
    TupleSet output_result;
    
    if (tuple_sets.size() > 1) {
        // 本次查询了多张表，需要做join操作
        // TupleSet output(output_schema);
        std::vector<Condition> remain_conditions;
        for (int i = 0; i < (int)selects.condition_num; i++) {
            const Condition &condition = selects.conditions[i];
            if (condition.left_is_attr && condition.right_is_attr &&
                condition.left_attr.relation_name != condition.right_attr.relation_name) {
                remain_conditions.push_back(condition);
            }
        }
        do_cartesian(tuple_sets, remain_conditions, output_result);
        output_result.set_schema(schema_result);
    } else {
        // 当前只查询一张表，直接返回结果即可
        // puts("FUCK YOU");
        
        TupleSet &output = tuple_sets.front();
        output_result.set_schema(schema_result);
        if (selects.groupby_num > 0){
            // printf("%d \n",output.size());
            output_result.clear();
            check_attr(selects, tables, schema_result);
            TupleSchema temp(schema_result);
            temp.append(groupby_schema);
            TupleSet ouput_after_group(temp);
            do_groupby(output, ouput_after_group, groupby_schema);
            printf("%d \n",schema_result.fields().size());
            ouput_after_group.set_schema(schema_result);
            // ouput_after_group.print(ss, false);
        }
        else
        {
            output_result = std::move(tuple_sets.front());
            // output_result.print(ss, tuple_sets.size() > 1);
        }
        
    }

    // group-by
    if (selects.groupby_num > 0) {
        // check_attr(selects, tables, output_schema);
        // TupleSchema temp(output_schema);
        // temp.append(group_by_schema);
        // TupleSet ouput_after_group(temp);
        // do_groupby(output_result, ouput_after_group, group_by_schema);
        // ouput_after_group.set_schema(output_schema);
    }

    // order-by
    if (selects.orderby_num != 0) {
        output_result.sort(selects.orderby_num, selects.orderby_attrs);
    }

    

    for (SelectExeNode *&tmp_node : select_nodes) {
        delete tmp_node;
    }

    std::stringstream ss;
    output_result.print(ss, tuple_sets.size() > 1);
    session_event->set_response(ss.str());
    end_trx_if_need(session, trx, true);
    return rc;
}

void ExecuteStage::do_cartesian(std::vector<TupleSet> &tuple_sets,
                                std::vector<Condition> &remain_conditions, TupleSet &result) {
    TupleSchema tmp;
    for (int i = tuple_sets.size() - 1; i >= 0; i--) {
        TupleSet &tuple_set = tuple_sets[i];
        tmp.append(tuple_set.get_schema());
    }
    tmp.append(result.schema());
    result = TupleSet(tmp);
    auto *values = new std::shared_ptr<TupleValue>[tmp.fields().size()];
    dfs(tuple_sets, remain_conditions, values, 0, result, tuple_sets.crbegin());
    delete[] values;
}

bool cmp(const TupleValue *left, const TupleValue *right, const CompOp &comp) {
    int result = left->compare(*right);
    if (comp == CompOp::EQUAL_TO) return result == 0;
    if (comp == CompOp::GREAT_EQUAL) return result >= 0;
    if (comp == CompOp::GREAT_THAN) return result > 0;
    if (comp == CompOp::LESS_EQUAL) return result <= 0;
    if (comp == CompOp::LESS_THAN) return result < 0;
    if (comp == CompOp::NOT_EQUAL) return result != 0;
    return false;
}

void ExecuteStage::dfs(std::vector<TupleSet> &tuple_sets, std::vector<Condition> &remain_conditions,
                       std::shared_ptr<TupleValue> *values, int value_num, TupleSet &result,
                       std::vector<TupleSet>::const_reverse_iterator cur) {
    auto schema = result.schema();
    if (value_num > 0) {
        for (auto i : remain_conditions) {
            auto left = i.left_attr, right = i.right_attr;
            int left_idx = schema.index_of_field(left.relation_name, left.attribute_name,
                                                 left.aggregation_type);
            int right_idx = schema.index_of_field(right.relation_name, right.attribute_name,
                                                  right.aggregation_type);
            if (left_idx >= value_num || right_idx >= value_num) {
                continue;
            }
            const TupleValue *left_v = values[left_idx].get();
            const TupleValue *right_v = values[right_idx].get();
            if (!cmp(left_v, right_v, i.comp)) {
                return;
            }
        }
    }
    if (cur == tuple_sets.crend()) {
        Tuple output_tuple;
        int output_num = schema.fields().size() - value_num;
        std::shared_ptr<TupleValue> output_value[output_num];
        for (int i = 0; i < output_num; i++) {
            auto field = schema.field(value_num + i);
            int val_idx = schema.index_of_field(field.table_name(), field.field_name(),
                                                field.aggregation_type());
            output_value[i] = values[val_idx];
        }
        for (int i = 0; i < output_num; i++) {
            output_tuple.add(output_value[i]);
        }
        result.merge(std::move(output_tuple));
    } else {
        for (size_t i = 0; i < cur->size(); i++) {
            auto &cur_tuple = cur->get(i);
            int cur_size = cur_tuple.size();
            for (int j = 0; j < cur_size; j++) {
                values[value_num + j] = cur_tuple.get_pointer(j);
            }
            dfs(tuple_sets, remain_conditions, values, value_num + cur_size, result, cur + 1);
        }
    }
}
bool match_table(const Selects &selects, const char *table_name_in_condition,
                 const char *table_name_to_match) {
    if (table_name_in_condition != nullptr) {
        return 0 == strcmp(table_name_in_condition, table_name_to_match);
    }

    return selects.relation_num == 1;
}

// 把所有的表和只跟这张表关联的condition都拿出来，生成最底层的select 执行节点
RC create_selection_executor(Trx *trx, Selects &selects, const char *db, const char *table_name,
                             SelectExeNode &select_node) {
    // 列出跟这张表关联的Attr
    TupleSchema schema;
    Table *table = DefaultHandler::get_default().find_table(db, table_name);
    if (nullptr == table) {
        LOG_WARN("No such table [%s] in db [%s]", table_name, db);
        return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    for (int i = selects.attr_num - 1; i >= 0; i--) {
        const RelAttr &attr = selects.attributes[i];
        if (nullptr == attr.relation_name || 0 == strcmp(table_name, attr.relation_name)) {
            if (0 == strcmp("*", attr.attribute_name)) {
                if (attr.aggregation_type != None) {
                    if (attr.aggregation_type != AggregationFunc::Count) {
                        LOG_WARN("Can't use * in this Aggregation");
                        return RC::MISMATCH;
                    }
                    schema.add(INTS, table->name(), attr.attribute_name, attr.aggregation_type);
                    continue;
                } else {
                    TupleSchema::from_table(table, schema);
                }
                break;
            } else if (attr.aggregation_type != None && attr.is_const) {  // count(1)
                if (attr.aggregation_type != AggregationFunc::Count) {
                    return RC::MISMATCH;
                }
                schema.add(INTS, table->name(), attr.attribute_name, attr.aggregation_type);
            } else {
                RC rc = schema_add_field(table, attr.attribute_name, attr.aggregation_type, schema);
                if (match_table(selects, attr.relation_name, table_name) && rc != RC::SUCCESS) {
                    return rc;
                }
            }
        }
    }

    // 找出仅与此表相关的过滤条件, 或者都是值的过滤条件
    std::vector<DefaultConditionFilter *> condition_filters;
    for (size_t i = 0; i < selects.condition_num; i++) {
        const Condition &condition = selects.conditions[i];
        if ((condition.left_is_attr == 0 && condition.right_is_attr == 0) ||  // 两边都是值
            (condition.left_is_attr == 1 && condition.right_is_attr == 0 &&
             match_table(selects, condition.left_attr.relation_name,
                         table_name)) ||  // 左边是属性右边是值
            (condition.left_is_attr == 0 && condition.right_is_attr == 1 &&
             match_table(selects, condition.right_attr.relation_name,
                         table_name)) ||  // 左边是值，右边是属性名
            (condition.left_is_attr == 1 && condition.right_is_attr == 1 &&
             match_table(selects, condition.left_attr.relation_name, table_name) &&
             match_table(selects, condition.right_attr.relation_name,
                         table_name))  // 左右都是属性名，并且表名都符合
        ) {
            DefaultConditionFilter *condition_filter = new DefaultConditionFilter();
            RC rc = condition_filter->init(*table, condition);
            if (rc != RC::SUCCESS) {
                delete condition_filter;
                for (DefaultConditionFilter *&filter : condition_filters) {
                    delete filter;
                }
                return rc;
            }
            condition_filters.push_back(condition_filter);
        } else if (condition.left_is_attr == 1 && condition.right_is_attr == 1 &&
                   condition.left_attr.relation_name != nullptr &&
                   condition.right_attr.relation_name != nullptr &&
                   condition.left_attr.relation_name != condition.right_attr.relation_name) {
            // 多表查询
            schema_add_field(table, condition.left_attr.attribute_name,
                             condition.left_attr.aggregation_type, schema);
            schema_add_field(table, condition.right_attr.attribute_name,
                             condition.right_attr.aggregation_type, schema);
        }
    }
    // orderby
    for (int i = selects.orderby_num - 1; i >= 0; i--) {
        OrderBy &order_by = selects.orderby_attrs[i];
        RelAttr &attr = order_by.attr;
        if (attr.relation_name == nullptr) {
            attr.relation_name = strdup(table->name());
        }
        schema_add_field(table, attr.attribute_name, AggregationFunc::None, schema);
    }
    if (selects.groupby_num > 0) {
        schema.clear();
        TupleSchema::from_table(table, schema);
    }
    return select_node.init(trx, table, std::move(schema), std::move(condition_filters));
}
