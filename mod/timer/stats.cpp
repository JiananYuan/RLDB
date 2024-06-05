//
// Created by yjn on 23-5-24.
//

#include "stats.h"
#include <cstdio>

namespace adgMod {

Stats* Stats::singleton = nullptr;

Stats::Stats() : timers(10, Timer{}), initial_time(system_clock::now()) {
  timers[0].setName("Mem");
  timers[1].setName("Imm");
  timers[2].setName("L0");
  timers[3].setName("L1");
  timers[4].setName("L2");
  timers[5].setName("L3");
  timers[6].setName("L4");
  timers[7].setName("L5");
  timers[8].setName("L6");
}

Stats* Stats::GetInstance() {
  if (!singleton) singleton = new Stats();
  return singleton;
}

void Stats::StartTimer(uint32_t id) {
  Timer& timer = timers[id];
  timer.Start();
}

uint8_t Stats::get_timer_size() {
  return timers.size();
}

std::pair<uint64_t, uint64_t> Stats::PauseTimer(uint32_t id, bool record) {
  Timer& timer = timers[id];
  return timer.Pause(record);
}

void Stats::ResetTimer(uint32_t id) {
  Timer& timer = timers[id];
  timer.Reset();
}

uint64_t Stats::ReportTime(uint32_t id, uint32_t count) {
  Timer& timer = timers[id];
  // convert to ns
  printf("Timer %d(%s) \t %f\n", id, timer.Name().data(), timer.Time() * 1.0 / count);
  return timer.Time();
}

void Stats::ReportTime() {
  for (int i = 0; i < timers.size(); ++i) {
    printf("Timer %u: %lu\n", i, timers[i].Time());
  }
}

uint64_t Stats::ReportTime(uint32_t id) {
  Timer& timer = timers[id];
  return timer.Time();
}

Stats::~Stats() {
  ReportTime();
}

uint64_t Stats::GetInitTimeDuration() const {
  return duration<uint64_t , std::nano>(system_clock::now() - initial_time).count();
}

void Stats::ResetAll() {
  for (Timer& t: timers) t.Reset();
  initial_time = system_clock::now();
}

}
