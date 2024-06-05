//
// Created by yjn on 23-5-24.
//

#ifndef MOD_STATS_H
#define MOD_STATS_H
#include <vector>
#include <cstdint>
#include <fstream>
#include <chrono>
#include "timer.h"
using namespace std::chrono;

namespace adgMod {

class Timer;
class Stats {
  static Stats* singleton;
  Stats();
  std::vector<Timer> timers;

 public:
  system_clock::time_point initial_time;
  static Stats* GetInstance();
  void StartTimer(uint32_t id);
  uint8_t get_timer_size();
  std::pair<uint64_t, uint64_t> PauseTimer(uint32_t id, bool record = false);
  void ResetTimer(uint32_t id);
  uint64_t ReportTime(uint32_t id, uint32_t count);
  void ReportTime();
  uint64_t ReportTime(uint32_t id);
  uint64_t GetInitTimeDuration() const;
  ~Stats();
  void ResetAll();
};

}

#endif
