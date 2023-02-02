module GraphBLAS.FSharp.Tests.Backend.Vector.ZeroCreate

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open Context
open TestCases
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Vector.zeroCreate.Tests"

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

    if vectorSize > 0 then
        let q = case.TestContext.Queue

        let (clVector: ClVector<'a>) =
            zeroCreate q HostInterop vectorSize case.Format

        let hostVector = clVector.ToHost q

        clVector.Dispose q

        checkResult vectorSize hostVector

let testFixtures (case: OperationCase<VectorFormat>) =
    let config = defaultConfig

    let getCorrectnessTestName dataType =
        $"Correctness on %A{dataType}, %A{case.Format}"

    let wgSize = 32
    let context = case.TestContext.ClContext

    let q = case.TestContext.Queue

    q.Error.Add(fun e -> failwithf "%A" e)

    [ let intZeroCreate = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest intZeroCreate
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let byteZeroCreat = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest byteZeroCreat
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")


      let floatZeroCreate = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest floatZeroCreate
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let boolZeroCreate = Vector.zeroCreate context wgSize

      case
      |> correctnessGenericTest boolZeroCreate
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

let tests =
    operationGPUTests "Backend.Vector.zeroCreate tests" testFixtures
