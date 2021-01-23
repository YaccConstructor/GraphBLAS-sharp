``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 7 SP1 (6.1.7601.0)
Intel Celeron CPU N2830 2.16GHz, 1 CPU, 2 logical and 2 physical cores
Frequency=2116035 Hz, Resolution=472.5820 ns, Timer=TSC
.NET Core SDK=3.1.402
  [Host]     : .NET Core 3.1.8 (CoreCLR 4.700.20.41105, CoreFX 4.700.20.41903), X64 RyuJIT DEBUG
  Job-OZGAKM : .NET Core 3.1.8 (CoreCLR 4.700.20.41105, CoreFX 4.700.20.41903), X64 RyuJIT

InvocationCount=1  RunStrategy=Throughput  UnrollFactor=1  

```
|                     Method |      PathToGraphPair |     Mean |   Error |  StdDev |        TEPS |      Min |      Max |
|--------------------------- |--------------------- |---------:|--------:|--------:|------------:|---------:|---------:|
| EWiseAdditionMathNetSparse | (arc1(...).mtx) [24] | 246.6 μs | 4.82 μs | 5.92 μs | 5198.210183 | 240.1 μs | 263.2 μs |
