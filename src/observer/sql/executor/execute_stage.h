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

#ifndef __OBSERVER_SQL_EXECUTE_STAGE_H__
#define __OBSERVER_SQL_EXECUTE_STAGE_H__

#include "common/seda/stage.h"
#include "rc.h"
#include "sql/executor/tuple.h"
#include "sql/parser/parse.h"
#include "storage/common/table.h"

class SessionEvent;

class ExecuteStage : public common::Stage {
public:
    ~ExecuteStage();
    static Stage *make_stage(const std::string &tag);

protected:
    // common function
    ExecuteStage(const char *tag);
    bool set_properties() override;

    bool initialize() override;
    void cleanup() override;
    void handle_event(common::StageEvent *event) override;
    void callback_event(common::StageEvent *event, common::CallbackContext *context) override;

    void handle_request(common::StageEvent *event);
    RC do_select(const char *db, Query *sql, SessionEvent *session_event);

protected:
private:
    Stage *default_storage_stage_ = nullptr;
    Stage *mem_storage_stage_ = nullptr;

    RC check_groupby(const Selects &selects, Table **tables);
    RC check_attr(const Selects &selects, Table **tables, TupleSchema &schema_result);
    RC init_select(const char *db, const Selects &selects, Table **tables,
                   TupleSchema &schema_result);
    RC do_cartesian(std::vector<TupleSet> &tuple_sets, std::vector<Condition> &remain_conditions,
                    TupleSet &result);
    RC dfs(std::vector<TupleSet> &tuple_sets, std::vector<Condition> &remain_conditions,
           std::shared_ptr<TupleValue> *values, int value_num, TupleSet &result,
           std::vector<TupleSet>::const_reverse_iterator);
};
#endif  //__OBSERVER_SQL_EXECUTE_STAGE_H__
