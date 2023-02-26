module GraphBLAS.FSharp.Tests.Backend.Vector.ZeroCreate

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests
open Context
open TestCases
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Vector.zeroCreate.Tests"

let config = Utils.defaultConfig

let wgSize = Utils.defaultWorkGroupSize

let checkResult size (actual: Vector<'a>) =
    Expect.equal actual.Size size "The size should be the same"

    match actual with
    | Vector.Dense vector ->
        Array.iter
        <| (fun item -> Expect.equal item None "values must be None")
        <| vector
    | Vector.Sparse vector ->
        Expect.equal vector.Values [| Unchecked.defaultof<'a> |] "The values array must contain the default value"
        Expect.equal vector.Indices [| 0 |] "The index array must contain the 0"

let correctnessGenericTest<'a when 'a: struct and 'a: equality>
    (zeroCreate: MailboxProcessor<_> -> AllocationFlag -> int -> VectorFormat -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (vectorSize: int)
    =

    let vectorSize = abs vectorSize

    if vectorSize > 0 then
        let q = case.TestContext.Queue

        let clVector =
            zeroCreate q HostInterop vectorSize case.Format

        let hostVector = clVector.ToHost q

        clVector.Dispose q

        checkResult vectorSize hostVector

let createTest<'a> case =
    let getCorrectnessTestName dataType =
        $"Correctness on %A{dataType}, %A{case.Format}"

    let context = case.TestContext.ClContext

    let intZeroCreate = Vector.zeroCreate context wgSize

    case
    |> correctnessGenericTest<int> intZeroCreate
    |> testPropertyWithConfig config (getCorrectnessTestName $"%A{typeof<'a>}")

let testFixtures case =
    let context = case.TestContext.ClContext
    let q = case.TestContext.Queue

    q.Error.Add(fun e -> failwithf "%A" e)

    [ createTest<int> case
      createTest<byte> case

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> case

      createTest<float32> case
      createTest<bool> case ]

let tests =
    operationGPUTests "Backend.Vector.zeroCreate tests" testFixtures
