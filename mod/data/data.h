#ifndef MOD_DATA_H
#define MOD_DATA_H
#include <unordered_map>

namespace adgMod {

// 记录每个level的查询延迟总和
extern std::unordered_map<int, double> level_query_latency;
// 记录每个level的查询次数
extern std::unordered_map<int, int> level_query_count;
// 记录单个查询的延迟
extern uint64_t query_latency;

}

#endif
