use clap::Parser;

use cache_cat::raft::network::client::RpcMultiClient;
use cache_cat::raft::network::model::{PrintTestReq, PrintTestRes};
use cache_cat::raft::network::pipeline_client::PipelineMultiClient;
use cache_cat::raft::types::entry::bae_operation::{BaseOperation, SetReq};
use cache_cat::raft::types::entry::request::Request;
use cache_cat::raft::types::raft_types::TypeConfig;
use openraft::raft::ClientWriteResponse;
use redis::RedisResult;
use redis::aio::MultiplexedConnection;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'T', long, default_value = "cachecat")]
    target: String,

    #[arg(short = 'm', long, default_value = "throughput")]
    mode: String,

    #[arg(short = 'o', long, default_value = "pwrite")]
    op: String,

    #[arg(short = 'c', long, default_value_t = 100)]
    count: usize,

    #[arg(short = 'n', long, default_value_t = 600)]
    clients: usize,

    #[arg(short = 't', long, default_value_t = 10000000)]
    total: usize,

    #[arg(short = 'e', long, default_value = "127.0.0.1:5001")]
    endpoints: String,

    #[arg(long, default_value_t = false)]
    redis: bool,

    #[arg(long, default_value = "127.0.0.1:6379")]
    redis_endpoints: String,

    #[arg(short = 'p', long, default_value_t = 100000)]
    warmup: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.redis {
        run_redis_benchmark(args).await
    } else {
        run_cachecat_benchmark(args).await
    }
}

async fn run_cachecat_benchmark(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let max_connections = if args.mode == "latency" {
        1
    } else {
        args.clients
    };

    println!(">>> 初始化连接池: {} 个连接 <<<", max_connections);

    let client = Arc::new(
        RpcMultiClient::connect_with_num(&args.endpoints, max_connections)
            .await
            .expect("连接失败，请检查端点是否可用"),
    );

    let pipeline_client = if args.op == "pwrite" {
        Some(Arc::new(
            PipelineMultiClient::connect(&args.endpoints, max_connections)
                .await
                .expect("pipeline 连接失败，请检查端点是否可用"),
        ))
    } else {
        None
    };

    if args.mode == "latency" {
        println!(">>> 延迟测试 - 请求数: {} <<<", args.count);
    } else {
        println!(
            ">>> 吞吐量测试 - {}并发/{}请求 <<<",
            args.clients, args.total
        );
        println!(">>> 预热阶段 - 发送 {} 个请求 <<<", args.warmup);

        run_engine(
            Arc::clone(&client),
            pipeline_client.clone(),
            args.clients,
            args.warmup,
            args.op.clone(),
            true,
        )
        .await;

        println!(">>> 预热完成，正式测试即将开始 <<<");
    }
    println!(
        "====== 性能测试开始 | Target: {} | Mode: {} | Op: {} ======",
        args.target, args.mode, args.op
    );

    if args.mode == "latency" {
        run_engine(
            Arc::clone(&client),
            pipeline_client.clone(),
            1,
            args.count,
            args.op,
            false,
        )
        .await;
    } else {
        run_engine(
            Arc::clone(&client),
            pipeline_client.clone(),
            args.clients,
            args.total,
            args.op,
            false,
        )
        .await;
    }

    Ok(())
}

async fn run_redis_benchmark(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let max_connections = if args.mode == "latency" {
        1
    } else {
        args.clients
    };

    println!(">>> 初始化 Redis 连接池: {} 个连接 <<<", max_connections);

    let client = Arc::new(
        connect_redis_with_num(&args.redis_endpoints, max_connections)
            .await
            .expect("Redis 连接失败，请检查端点是否可用"),
    );

    if args.mode == "latency" {
        println!(">>> Redis 延迟测试 - 请求数: {} <<<", args.count);
    } else {
        println!(
            ">>> Redis 吞吐量测试 - {}并发/{}请求 <<<",
            args.clients, args.total
        );
        println!(">>> 预热阶段 - 发送 {} 个请求 <<<", args.warmup);

        run_redis_engine(
            Arc::clone(&client),
            args.clients,
            args.warmup,
            args.op.clone(),
            true,
        )
        .await;

        println!(">>> 预热完成，正式测试即将开始 <<<");
    }
    println!(
        "====== 性能测试开始 | Target: redis | Endpoints: {} | Mode: {} | Op: {} ======",
        args.redis_endpoints, args.mode, args.op
    );

    if args.mode == "latency" {
        run_redis_engine(Arc::clone(&client), 1, args.count, args.op, false).await;
    } else {
        run_redis_engine(
            Arc::clone(&client),
            args.clients,
            args.total,
            args.op,
            false,
        )
        .await;
    }

    Ok(())
}

async fn connect_redis_with_num(
    endpoints: &str,
    num: usize,
) -> RedisResult<Vec<MultiplexedConnection>> {
    let urls = parse_redis_endpoints(endpoints);
    let mut conns = Vec::with_capacity(num);

    for i in 0..num {
        let client = redis::Client::open(urls[i % urls.len()].as_str())?;
        conns.push(client.get_multiplexed_async_connection().await?);
    }

    Ok(conns)
}

fn parse_redis_endpoints(endpoints: &str) -> Vec<String> {
    let urls: Vec<String> = endpoints
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .map(normalize_redis_endpoint)
        .collect();

    if urls.is_empty() {
        vec![normalize_redis_endpoint("127.0.0.1:6379")]
    } else {
        urls
    }
}

fn normalize_redis_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint.trim();
    if endpoint.starts_with("redis://") || endpoint.starts_with("rediss://") {
        endpoint.to_string()
    } else {
        format!("redis://{}/", endpoint.trim_end_matches('/'))
    }
}

async fn run_engine(
    client: Arc<RpcMultiClient>,
    pipeline_client: Option<Arc<PipelineMultiClient>>,
    client_num: usize,
    total_tasks: usize,
    op_type: String,
    is_warmup: bool,
) {
    // --- 优化点 1: 数据结构变更 ---
    // 仅用于最终报告的详细数据（为了不影响实时性能，我们只在工作线程内部维护，最后合并）
    let final_latencies = Arc::new(Mutex::new(Vec::with_capacity(total_tasks)));

    // 实时统计用的原子变量 (无锁，极低开销)
    let ops = Arc::new(AtomicI64::new(0));
    let total_dur_us = Arc::new(AtomicU64::new(0)); // 存储总微秒数

    let start_time = Instant::now();
    let mut handles = vec![];

    // 进度条监控协程
    let ops_clone = Arc::clone(&ops);
    let dur_clone = Arc::clone(&total_dur_us);
    let total_tasks_f = total_tasks as f64;

    if !is_warmup {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let curr = ops_clone.load(Ordering::Relaxed);
                if curr >= total_tasks as i64 {
                    break;
                }

                let curr_ops = curr as u64;
                let curr_dur_us = dur_clone.load(Ordering::Relaxed);

                let avg_str = if curr_ops > 0 {
                    format!("{:?}", Duration::from_micros(curr_dur_us / curr_ops))
                } else {
                    "N/A".to_string()
                };

                print!(
                    "\r进度: {}/{} ({:.1}%) | 实时TPS: {:.2} | 平均延迟: {}",
                    curr,
                    total_tasks,
                    (curr as f64 / total_tasks_f) * 100.0,
                    curr as f64 / start_time.elapsed().as_secs_f64(),
                    avg_str
                );
                use std::io::{self, Write};
                io::stdout().flush().unwrap();
            }
        });
    }

    let tasks_per_client = total_tasks / client_num;

    for _cid in 0..client_num {
        let latencies_c = Arc::clone(&final_latencies);
        let ops_c = Arc::clone(&ops);
        let dur_c = Arc::clone(&total_dur_us);
        let client_c = Arc::clone(&client);
        let pipeline_client_c = pipeline_client.clone();

        let op = op_type.clone();
        let mode = if client_num == 1 {
            "latency"
        } else {
            "throughput"
        };

        let handle = tokio::spawn(async move {
            let mut local_latencies = Vec::with_capacity(tasks_per_client);

            for i in 0..tasks_per_client {
                let start = Instant::now();
                let success;

                if op == "write" {
                    let res: Result<ClientWriteResponse<TypeConfig>, _> = client_c
                        .call(
                            2,
                            Request::Base(BaseOperation::Set(SetReq {
                                key: i.to_string().into_bytes().into(),
                                value: Vec::from("xxx").into(),
                                ex_time: 0,
                            })),
                        )
                        .await;
                    success = res.is_ok();
                } else if op == "pwrite" {
                    let pipeline_client = pipeline_client_c
                        .as_ref()
                        .expect("pwrite 模式需要 pipeline client");

                    let res: Result<ClientWriteResponse<TypeConfig>, _> = pipeline_client
                        .call(Request::Base(BaseOperation::Set(SetReq {
                            key: Arc::from(format!("test{}", i).into_bytes()),
                            value: Arc::from(format!("test_value_{}", i).into_bytes()),
                            ex_time: 0,
                        })))
                        .await;
                    success = res.is_ok();
                } else {
                    let res: Result<PrintTestRes, _> = client_c
                        .call(
                            1,
                            PrintTestReq {
                                message: String::from("xxx"),
                            },
                        )
                        .await;
                    success = res.is_ok();
                }

                if success {
                    let duration = start.elapsed();

                    // --- 优化点 3: 更新原子变量 ---
                    // 累加微秒数，用于实时平均计算
                    dur_c.fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
                    ops_c.fetch_add(1, Ordering::Relaxed);

                    // 仍然存入本地数组，用于最终报告的 P99 计算
                    local_latencies.push(duration);

                    if local_latencies.len() >= 1000 {
                        let mut global = latencies_c.lock().await;
                        global.extend(local_latencies.drain(..));
                    }
                }

                if mode == "latency" {
                    sleep(Duration::from_millis(2)).await;
                }
            }

            // 剩余数据刷入
            if !local_latencies.is_empty() {
                let mut global = latencies_c.lock().await;
                global.extend(local_latencies);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start_time.elapsed();
    print!("\r"); // 清除当前行的进度显示

    if !is_warmup {
        let final_lats = final_latencies.lock().await;
        print_stats(&final_lats, elapsed, total_tasks);
    }
}

async fn run_redis_engine(
    client: Arc<Vec<MultiplexedConnection>>,
    client_num: usize,
    total_tasks: usize,
    op_type: String,
    is_warmup: bool,
) {
    let final_latencies = Arc::new(Mutex::new(Vec::with_capacity(total_tasks)));
    let ops = Arc::new(AtomicI64::new(0));
    let total_dur_us = Arc::new(AtomicU64::new(0));

    let start_time = Instant::now();
    let mut handles = vec![];

    let ops_clone = Arc::clone(&ops);
    let dur_clone = Arc::clone(&total_dur_us);
    let total_tasks_f = total_tasks as f64;

    if !is_warmup {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let curr = ops_clone.load(Ordering::Relaxed);
                if curr >= total_tasks as i64 {
                    break;
                }

                let curr_ops = curr as u64;
                let curr_dur_us = dur_clone.load(Ordering::Relaxed);

                let avg_str = if curr_ops > 0 {
                    format!("{:?}", Duration::from_micros(curr_dur_us / curr_ops))
                } else {
                    "N/A".to_string()
                };

                print!(
                    "\r进度: {}/{} ({:.1}%) | 实时TPS: {:.2} | 平均延迟: {}",
                    curr,
                    total_tasks,
                    (curr as f64 / total_tasks_f) * 100.0,
                    curr as f64 / start_time.elapsed().as_secs_f64(),
                    avg_str
                );
                use std::io::{self, Write};
                io::stdout().flush().unwrap();
            }
        });
    }

    let tasks_per_client = total_tasks / client_num;

    for cid in 0..client_num {
        let latencies_c = Arc::clone(&final_latencies);
        let ops_c = Arc::clone(&ops);
        let dur_c = Arc::clone(&total_dur_us);
        let mut conn = client[cid % client.len()].clone();

        let op = op_type.clone();
        let mode = if client_num == 1 {
            "latency"
        } else {
            "throughput"
        };

        let handle = tokio::spawn(async move {
            let mut local_latencies = Vec::with_capacity(tasks_per_client);

            for i in 0..tasks_per_client {
                let start = Instant::now();
                let success;

                if op == "write" || op == "pwrite" {
                    let key = if op == "pwrite" {
                        format!("test{}", i)
                    } else {
                        i.to_string()
                    };
                    let value = if op == "pwrite" {
                        format!("test_value_{}", i)
                    } else {
                        String::from("xxx")
                    };

                    let res: RedisResult<()> = redis::cmd("SET")
                        .arg(&key)
                        .arg(&value)
                        .query_async(&mut conn)
                        .await;
                    success = res.is_ok();
                } else {
                    let key = i.to_string();
                    let res: RedisResult<Option<Vec<u8>>> =
                        redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                    success = res.is_ok();
                }

                if success {
                    let duration = start.elapsed();

                    dur_c.fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
                    ops_c.fetch_add(1, Ordering::Relaxed);
                    local_latencies.push(duration);

                    if local_latencies.len() >= 1000 {
                        let mut global = latencies_c.lock().await;
                        global.extend(local_latencies.drain(..));
                    }
                }

                if mode == "latency" {
                    sleep(Duration::from_millis(2)).await;
                }
            }

            if !local_latencies.is_empty() {
                let mut global = latencies_c.lock().await;
                global.extend(local_latencies);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start_time.elapsed();
    print!("\r");

    if !is_warmup {
        let final_lats = final_latencies.lock().await;
        print_stats(&final_lats, elapsed, total_tasks);
    }
}

fn print_stats(d: &[Duration], elapsed: Duration, total_req: usize) {
    if d.is_empty() {
        println!("\n❌ 无成功样本");
        return;
    }

    let mut sorted_d = d.to_vec();
    sorted_d.sort();

    let sum: Duration = sorted_d.iter().sum();
    let avg = sum / sorted_d.len() as u32;

    println!("\n---------------- 测试结果 ----------------");
    println!("完成/总计:    {}/{}", sorted_d.len(), total_req);
    println!("总运行耗时:   {:.2}s", elapsed.as_secs_f64());
    println!(
        "平均吞吐量:   {:.2} req/s",
        sorted_d.len() as f64 / elapsed.as_secs_f64()
    );
    println!("\n--- 延迟分布 ---");
    println!("最小值: {:?}", sorted_d[0]);
    println!("平均值: {:?}", avg);
    println!(
        "中位数: {:?}",
        sorted_d[(sorted_d.len() as f64 * 0.5) as usize]
    );
    println!(
        "P99:    {:?}",
        sorted_d[(sorted_d.len() as f64 * 0.99) as usize]
    );
    println!("最大值: {:?}", sorted_d[sorted_d.len() - 1]);
    println!("------------------------------------------");
}
