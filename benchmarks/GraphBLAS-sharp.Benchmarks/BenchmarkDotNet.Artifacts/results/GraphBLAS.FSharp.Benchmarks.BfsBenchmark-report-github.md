``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 7 SP1 (6.1.7601.0)
Intel Celeron CPU N2830 2.16GHz, 1 CPU, 2 logical and 2 physical cores
Frequency=2115908 Hz, Resolution=472.6103 ns, Timer=TSC
.NET Core SDK=3.1.302
  [Host] : .NET Core 3.1.8 (CoreCLR 4.700.20.41105, CoreFX 4.700.20.41903), X64 RyuJIT DEBUG

IterationCount=10  

```
|   Method |          PathToGraph | Mean | Error |                           TEPS |
|--------- |--------------------- |-----:|------:|-------------------------------:|
| LevelBFS | Datas(...)r.mtx [32] |   NA |    NA | (&quot;227320&quot;, &quot;227320&quot;, &quot;814134&quot;) |

Benchmarks with issues:
  BfsBenchmark.LevelBFS: Job-WTZPYK(IterationCount=10) [PathToGraph=Datas(...)r.mtx [32]]
