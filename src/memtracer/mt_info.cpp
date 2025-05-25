/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
#include "memtracer.h"
#include "mt_info.h"

namespace memtracer {

mt_visible size_t allocated_memory() { return MT.allocated_memory(); }

mt_visible size_t meta_memory() { return MT.meta_memory(); }

mt_visible size_t memory_limit() { return MT.memory_limit(); }
}  // namespace memtracer
