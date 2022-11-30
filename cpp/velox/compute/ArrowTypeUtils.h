/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include "velox/type/Type.h"

using namespace facebook::velox;

std::shared_ptr<arrow::DataType> toArrowTypeFromName(const std::string& type_name);

std::shared_ptr<arrow::DataType> toArrowType(const TypePtr& type);

const char* arrowTypeIdToFormatStr(arrow::Type::type typeId);

std::shared_ptr<arrow::Schema> toArrowSchema(const std::shared_ptr<const RowType>& row_type);
