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

let wgSize = 32

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

let testFixtures (case: OperationCase<VectorFormat>) =

    let getCorrectnessTestName dataType =
        $"Correctness on %A{dataType}, %A{case.Format}"

    let context = case.TestContext.ClContext

    let q = case.TestContext.Queue

    q.Error.Add(fun e -> failwithf "%A" e)

    [ let intZeroCreate = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest<int> intZeroCreate
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let byteZeroCreat = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest<byte> byteZeroCreat
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      if Utils.isFloat64Available context.ClDevice then
          let floatZeroCreate = Vector.zeroCreate context wgSize

          case
          |> correctnessGenericTest<float> floatZeroCreate
          |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let float32ZeroCreate = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest<float32> float32ZeroCreate
      |> testPropertyWithConfig config (getCorrectnessTestName "float32")

      let boolZeroCreate = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest<bool> boolZeroCreate
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

let tests =
    operationGPUTests "Backend.Vector.zeroCreate tests" testFixtures
