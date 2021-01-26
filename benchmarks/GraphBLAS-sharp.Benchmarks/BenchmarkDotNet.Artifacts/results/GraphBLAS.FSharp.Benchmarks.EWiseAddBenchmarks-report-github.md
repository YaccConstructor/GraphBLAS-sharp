``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 7 SP1 (6.1.7601.0)
Intel Celeron CPU N2830 2.16GHz, 1 CPU, 2 logical and 2 physical cores
Frequency=2115966 Hz, Resolution=472.5974 ns, Timer=TSC
.NET Core SDK=3.1.402
  [Host]     : .NET Core 3.1.8 (CoreCLR 4.700.20.41105, CoreFX 4.700.20.41903), X64 RyuJIT DEBUG
  Job-KQHVJB : .NET Core 3.1.8 (CoreCLR 4.700.20.41105, CoreFX 4.700.20.41903), X64 RyuJIT

InvocationCount=1  RunStrategy=Throughput  UnrollFactor=1  

```
|                     Method |      PathToGraphPair |           Mean |        Error |       StdDev |        TEPS |            Min |            Max |
|--------------------------- |--------------------- |---------------:|-------------:|-------------:|------------:|---------------:|---------------:|
|           EWiseAdditionCOO | (arc1(...).mtx) [24] | 2,092,343.0 μs | 35,174.33 μs | 31,181.13 μs |    0.612710 | 2,056,612.0 μs | 2,164,915.2 μs |
| EWiseAdditionMathNetSparse | (arc1(...).mtx) [24] |       247.2 μs |      3.12 μs |      2.60 μs | 5186.746479 |       243.4 μs |       252.8 μs |
