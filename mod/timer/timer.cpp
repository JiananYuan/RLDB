//
// Created by yjn on 23-5-24.
//

#include "timer.h"
#include <cassert>
#include <utility>
using namespace std::chrono;

namespace adgMod {

Timer::Timer(): time_accumulated(0), started(false) {}

Timer::Timer(std::string _name): time_accumulated(0), started(false), name(std::move(_name)) {}

void Timer::setName(std::string _name) {
  name = std::move(_name);
}

std::string Timer::Name() {
  return name;
}

void Timer::Start() {
  if (started) {
    return;
  }
  // assert(!started);
  time_started = system_clock::now();
  started = true;
}

void Timer::Reset() {
  time_accumulated = 0;
  started = false;
}

uint64_t Timer::Time() {
  return time_accumulated;
}

std::pair<uint64_t, uint64_t> Timer::Pause(bool record) {
  assert(started);
  auto time_elapse = duration<double, std::nano>(system_clock::now() - time_started).count();
  time_accumulated += time_elapse;
  if (record) {
    //    Stats* instance = Stats::GetInstance();
    //    uint64_t start_absolute = time_started - instance->init_time;
    //    uint64_t end_absolute = start_absolute + time_elapse;
    //    started = false;
    //    return {start_absolute / frequency, end_absolute / frequency};
  } else {
    started = false;
    return {0, 0};
  }
  return {0, 0};
}

}
