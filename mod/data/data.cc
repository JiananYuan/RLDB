#include "data.h"

namespace adgMod {

std::unordered_map<int, double> level_query_latency;
std::unordered_map<int, int> level_query_count;
uint64_t query_latency = 0;

}