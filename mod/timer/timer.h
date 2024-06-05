//
// Created by yjn on 23-5-24.
//

#ifndef MOD_TIMER_H
#define MOD_TIMER_H
#include <cstdint>
#include <iostream>
#include <chrono>
using namespace std::chrono;

namespace adgMod {

class Timer {
  system_clock::time_point time_started;
  uint64_t time_accumulated;
  bool started;
  std::string name;

 public:
  void Start();
  std::pair<uint64_t, uint64_t> Pause(bool record = false);
  void Reset();
  uint64_t Time();
  void setName(std::string);
  std::string Name();

  Timer();
  Timer(std::string);
  ~Timer() = default;
};

}

#endif
