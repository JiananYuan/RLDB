#include <chrono>
#include <fstream>
#include <iostream>
#include <vector>
#include <sstream>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "mod/util/cxxopts.hpp"
#include "mod/timer/stats.h"
#include "leveldb/env.h"
#include "helpers/memenv/memenv.h"
#include "mod/timer/stats.h"
#include "mod/data/data.h"
#include "leveldb/filter_policy.h"

using namespace leveldb;
using namespace std;
using namespace std::chrono;
const uint64_t M = 1000000;
uint32_t key_size, value_size;

string generate_key(const string& key) {
  return key;
  string result = string(key_size - key.length(), '0') + key;
  return std::move(result);
}

string generate_value(uint64_t value) {
  string value_string = to_string(value);
  string result = string(value_size - value_string.length(), '0') + value_string;
  return std::move(result);
}

int main(int argc, char** argv) {
  // system("sync; echo 3 | sudo tee /proc/sys/vm/drop_caches");
  // print now time
  auto now = std::chrono::system_clock::now();
  std::time_t current_time = std::chrono::system_clock::to_time_t(now);
  char buffer[80];
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&current_time));
  std::cout << "[START TIME] " << buffer << std::endl;

  // READ FROM ARGS
  std::string db_path, write_type, ds_name, exp_log_file;
  double num_data, num_ops;
  const string dir_path = "/home/yjn/Desktop/VLDB/Dataset/";
  const std::string kWrite = "W";
  const std::string kRead = "R";
  int start_type, end_type;
  bool range_query;

  cxxopts::Options commandline_options("motivation", "Testing query time of different layer.");
  commandline_options.add_options()
    ("h,help", "print help message", cxxopts::value<bool>()->default_value("false"))
    ("p,db_path", "path of the database", cxxopts::value<std::string>(db_path)->default_value("test"))
    ("n,number_data", "data number (million)", cxxopts::value<double>(num_data)->default_value("64"))
    ("m,number_ops", "operation number (million)", cxxopts::value<double>(num_ops)->default_value("10"))
    ("w,write_type", "write rand or seq", cxxopts::value<std::string>(write_type)->default_value("rand"))
    ("d,dataset_name", "name of the dataset", cxxopts::value<std::string>(ds_name)->default_value("books"))
    ("k, key_size", "byte size of the key", cxxopts::value<uint32_t>(key_size)->default_value("20"))
    ("v, value_size", "byte size of the value", cxxopts::value<uint32_t>(value_size)->default_value("500"))
    ("o, output_file", "path of result output file", cxxopts::value<std::string>(exp_log_file)->default_value("log"))
    ("r, range_query", "use range query", cxxopts::value<bool>(range_query)->default_value("false"))
    ("s, start_type", "start type id", cxxopts::value<int>(start_type)->default_value("3"))
    ("f, end_type", "end type id", cxxopts::value<int>(end_type)->default_value("4"));
  auto result = commandline_options.parse(argc, argv);
  if (result.count("help")) {
    printf("%s", commandline_options.help().c_str());
    exit(0);
  }
  std::ofstream exp_log(exp_log_file);
  const std::string ds_rand_path = dir_path + ds_name + "/" + ds_name + "_rand_data.csv";
  const std::string ds_seq_path = dir_path + ds_name + "/" + ds_name + "_seq_data.csv";
  const std::string ds_rand_query = dir_path + ds_name + "/" + ds_name + "_rand_query.csv";
  const std::string ds_zipf_query = dir_path + ds_name + "/" + ds_name + "_zipf_query.csv";
  const std::string ds_read_heavy = dir_path + ds_name + "/" + ds_name + "_read_heavy.csv";
  const std::string ds_rw_balance = dir_path + ds_name + "/" + ds_name + "_rw_balance.csv";
  const std::string ds_write_heavy = dir_path + ds_name + "/" + ds_name + "_write_heavy.csv";

  // REMOVE OLD DATABASE
  // system(("rm -rf " + db_path).data());

  // CONFIG LEADER-TREE
  DB* db;
  Options options;
  options.create_if_missing = true;
  // options.max_file_size = 64 * 1024 * 1024;
  // options.write_buffer_size = 64 * 1024 * 1024;
  // options.compression = kNoCompression;
  // options.paranoid_checks = false;
	options.filter_policy = NewBloomFilterPolicy(10);

  ReadOptions read_options = ReadOptions();
  WriteOptions write_options = WriteOptions();
  write_options.sync = false;

  // OPEN OR BUILD DATABASE
  const uint64_t kNum = num_data * M;
  const uint64_t kOps = num_ops * M;
  std::cout << "[1/4] opening database..." << std::endl;
  Status status = DB::Open(options, db_path, &db);

  // READ DATA
  double bulkload_time = 0;
  int print_interval = kNum / 128;
  std::ifstream in;
  if (write_type == "rand")   in.open(ds_rand_path);
  if (write_type == "seq")    in.open(ds_seq_path);

  // TEST PUT()
  std::cout << "[2/4] bulkloading data... " << std::endl;
  for (int i = 0; i < kNum; i += 1) {
    if (i % print_interval == 0) {
      printf("\rprogress : %.2f%%", 100.0 * i / kNum);
      fflush(stdout);
    }
    std::string key;
    in >> key;
    key = generate_key(key);
    string val = generate_value(strtoull(key.c_str(), nullptr, 10));
    // auto st_time = high_resolution_clock::now();
    status = db->Put(write_options, key, val);
    // auto end_time = high_resolution_clock::now();
    // bulkload_time += duration_cast<nanoseconds>(end_time - st_time).count();
  }
  std::cout << std::endl;
  // std::cout << "bulkload time : " << static_cast<uint64_t>(bulkload_time) / 1e9 << " s" << std::endl;
  // exp_log << "bulkload time : " << static_cast<uint64_t>(bulkload_time) / 1e9 << " s" << std::endl;
  in.close();

	for (int ei = start_type; ei < end_type; ei += 1) {
		switch (ei) {
      case 0:
        std::cout << "[3/4] testing-1 (write-more)... " << std::endl;
        in.open(ds_write_heavy);
        break;
      case 1:
        std::cout << "[3/4] testing-2 (rw_balance)... " << std::endl;
        in.open(ds_rw_balance);
        break;
      case 2:
        std::cout << "[3/4] testing-3 (read_more)... " << std::endl;
        in.open(ds_read_heavy);
        break;
      case 3:
        std::cout << "[3/4] testing-4 (read-only)... " << std::endl;
        in.open(ds_zipf_query);
				// in.open(ds_rand_query);
        break;
    }
		double ops_time = 0;
  	std::vector<double> latencies;
		// remember that when `read_type` is set to `rand` or `zipf`, the workload is read-only
		print_interval = kOps / 128;
		auto st_time = high_resolution_clock::now();
		auto end_time = high_resolution_clock::now();
		uint32_t num_real_ops = 0;
		bool exist_not_found = false;
		for (uint64_t i = 0; i < kOps; i += 1) {
			adgMod::query_latency = 0;
			if (i % print_interval == 0) {
				printf("\rprogress : %.2f%%", 100.0 * i / kOps);
				fflush(stdout);
			}
			// if `read_type` equal to range, we perform `kNumRange` range queries
			std::string line;
			getline(in, line);
			std::stringstream ss(line);
			ss >> line;
			if (line == kWrite) {
				ss >> line;  // key
				line = generate_key(line);
				string val = generate_value(strtoull(line.c_str(), nullptr, 10));
				st_time = high_resolution_clock::now();
				status = db->Put(write_options, line, val);
				end_time = high_resolution_clock::now();
			} else if (line == kRead) {
				ss >> line;  // key
				line = generate_key(line);
        if (range_query) {
          leveldb::ReadOptions iter_option;
          iter_option.fill_cache = false;
          auto iter = db->NewIterator(iter_option);
          const int range_length = 100;
          uint32_t cnt = 0;
          st_time = high_resolution_clock::now();
          for (iter->Seek(line); iter->Valid() && cnt < range_length; iter->Next()) {
            cnt += 1;
          }
          end_time = high_resolution_clock::now();
          delete iter;
        } else {
          string val;
          st_time = high_resolution_clock::now();
          status = db->Get(read_options, line, &val);
				  end_time = high_resolution_clock::now();
          if (status.IsNotFound())    exist_not_found = true;
        }
			}
			auto duration = duration_cast<nanoseconds>(end_time - st_time).count();
			latencies.push_back(duration);
			ops_time += duration;
			num_real_ops += 1;
		}
		if (kOps != 0) {
			std::cout << "\n";
			std::sort(latencies.begin(), latencies.end());
			if (exist_not_found)    std::cout << "exist not found" << std::endl;
			std::cout << "average operate time : " << ops_time / num_real_ops << " ns" << std::endl;
			// exp_log << "average operate time : " << ops_time / num_real_ops << " ns" << std::endl;
			// std::cout << "average operate throughput : " << num_real_ops / (ops_time / 1e9) << " ops" << std::endl;
			// exp_log << "average operate throughput : " << num_real_ops / (ops_time / 1e9) << " ops" << std::endl;
			double p99_latency = latencies[static_cast<uint32_t>(num_real_ops * 0.99)];
			std::cout << "average p99 operate time : " << p99_latency << " ns" << std::endl;
			// exp_log << "average p99 operate time : " << p99_latency << " ns" << std::endl;

			// auto ins = adgMod::Stats::GetInstance();
			// for (uint8_t i = 0; i < 9; i += 1) {
			// 	ins->ReportTime(i, adgMod::level_query_count[i]);
			// }
			for (uint8_t i = 0; i < 10; i += 1) {
				if (i == 0) {
					std::cout << "Mem: #" << adgMod::level_query_count[i] << " ";
				} else if (i == 1) {
					std::cout << "Imm: #" << adgMod::level_query_count[i] << " ";
				} else if (i >= 2 && i < 9) {
					std::cout << "L" << i - 2 << ": #" << adgMod::level_query_count[i] << " ";
				} else {
					std::cout << "Not Found: #" << adgMod::level_query_count[i] << " ";
				}
				std::cout << adgMod::level_query_latency[i] / adgMod::level_query_count[i] << " ns" << std::endl;
			}
		}
		if (in.is_open())   in.close();
	}
  std::cout << "[4/4] closing database..." << std::endl;
  delete db;
  exp_log.close();
  now = std::chrono::system_clock::now();
  current_time = std::chrono::system_clock::to_time_t(now);
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&current_time));
  std::cout << "[END TIME] " << buffer << std::endl;
  return 0;
}
